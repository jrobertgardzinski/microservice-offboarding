package com.jrobertgardzinski.offboarding.infrastructure;

import com.jrobertgardzinski.offboarding.application.SagaStore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The real transport around the pure {@link EventsRouter}: one consumer polls the deletion fact
 * and every participant topic, the shared (thread-safe) producer publishes whatever the router
 * answers plus the sweeper's announcements. The correlation id rides the Kafka header, in and
 * out, so the async hops keep the trace of the request that started the deletion.
 *
 * <p>Supervision sits around one PASS, never around the while: an infrastructure error (broker
 * away, database down) rewinds to the committed offsets, backs off — one second doubling to
 * thirty — and retries; the loops themselves never die. Each successful pass stamps a readiness
 * flag that /health watches ({@link #healthy}), and each ITERATION — failing ones included —
 * stamps a liveness heartbeat that /alive watches ({@link #alive}): an outage makes the service
 * unready, only a thread that stopped being scheduled makes it dead. Within a pass the order is
 * the at-least-once guarantee:
 * outcomes DEMONSTRABLY reach the broker (flush, then every send's future checked — flush alone
 * reports no delivery errors), then the outbox marks them announced, then — and only then — the
 * offset commits. Any failed send fails the whole pass: nothing announced for it, no offsets
 * committed, the rewind-and-retry path picks the batch up again. A crash between any two steps
 * re-delivers or re-publishes, and the idempotent saga transitions and consumers absorb the
 * duplicates.
 */
public class KafkaLoop {

    static final String CID_HEADER = "X-Correlation-Id";

    /** The longest a loop legitimately pauses between iterations: the retry backoff's cap. */
    static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    /**
     * The producer's delivery clocks, set EXPLICITLY because /alive depends on them: a broker
     * outage with records in the buffer makes {@code flush()} (and a blocked {@code send()})
     * hold a loop iteration for up to {@code delivery.timeout.ms} — with Kafka's default of
     * 120s one such iteration would outlast the /alive stall tolerance (same default 120s) and
     * a mere broker outage would read as a dead thread, restarting a pod a restart cannot fix.
     * 30s bounds the block well inside the tolerance; {@link Main#ALIVE_STALL_FLOOR} is derived
     * from these same constants so the two can never drift apart again.
     */
    static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    /** One in-flight request's timeout; two of these fit inside {@link #DELIVERY_TIMEOUT}. */
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(15);
    /** How long send() may block on metadata — the same bound as the delivery timeout. */
    static final Duration MAX_BLOCK = Duration.ofSeconds(30);

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLoop.class);
    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = MAX_BACKOFF.toMillis();

    private final EventsRouter router;
    private final SagaStore store;
    private final Collection<String> topics;
    private final Duration sweepEvery;
    private final Duration deliveryTimeout;
    private final Duration requestTimeout;
    private final Duration maxBlock;

    private volatile boolean running = true;
    private volatile KafkaConsumer<String, String> consumer;
    private volatile KafkaProducer<String, String> producer;
    // the readiness flags: /health answers 503 once a loop stops completing passes (see healthy()).
    // System.nanoTime, not currentTimeMillis: the markers measure elapsed time, and the wall
    // clock can jump (NTP step) — backwards would fake a 503, forwards would mask a real stall
    private volatile long lastConsumerPassNanos;
    private volatile long lastSweeperPassNanos;
    // the liveness heartbeats behind /alive: stamped at the START of every loop iteration — the
    // failing ones included — so they keep beating through a broker outage's backoff and stop
    // only when a thread genuinely stops being scheduled (see alive())
    private volatile long lastConsumerBeatNanos;
    private volatile long lastSweeperBeatNanos;
    private Thread consumerThread;
    private Thread sweeperThread;
    private final AtomicBoolean started = new AtomicBoolean();

    public KafkaLoop(EventsRouter router, SagaStore store, Collection<String> topics,
                     Duration sweepEvery) {
        this(router, store, topics, sweepEvery, DELIVERY_TIMEOUT, REQUEST_TIMEOUT, MAX_BLOCK);
    }

    /**
     * Test seam: the broker-outage test shrinks the producer's delivery clocks so proving "the
     * beat outlives the outage" takes seconds, not the production thirty per blocked send.
     */
    KafkaLoop(EventsRouter router, SagaStore store, Collection<String> topics, Duration sweepEvery,
              Duration deliveryTimeout, Duration requestTimeout, Duration maxBlock) {
        this.router = router;
        this.store = store;
        this.topics = topics;
        this.sweepEvery = sweepEvery;
        this.deliveryTimeout = deliveryTimeout;
        this.requestTimeout = requestTimeout;
        this.maxBlock = maxBlock;
    }

    /**
     * Starts the consuming loop and the timeout sweeper, each on its own daemon virtual thread.
     * One start per instance: a second call throws — it would overwrite the producer and thread
     * fields while the first loops still run, leaking the old producer and orphaning threads that
     * shutdown() could no longer reach.
     */
    public void start(String bootstrapServers) {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("this KafkaLoop is already started; "
                    + "create a new instance instead of starting one twice");
        }
        // count readiness and liveness from here, so a service still warming up is not born dead
        long now = System.nanoTime();
        lastConsumerPassNanos = now;
        lastSweeperPassNanos = now;
        lastConsumerBeatNanos = now;
        lastSweeperBeatNanos = now;
        producer = new KafkaProducer<>(producerProps(bootstrapServers));
        consumerThread = Thread.ofVirtual().name("offboarding-consumer")
                .start(() -> consume(bootstrapServers));
        sweeperThread = Thread.ofVirtual().name("offboarding-sweeper").start(this::sweep);
        // the started CAS above also makes this exactly one hook per instance — several would
        // run shutdown() over each other and leak threads waiting on the same loop
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "offboarding-loop-shutdown"));
    }

    /**
     * READINESS: true while both loops keep COMPLETING passes within their stall tolerances — a
     * loop alive but unable to finish a pass (broker away, database down) reads unhealthy, which
     * is what gates dependants and routing.
     */
    public boolean healthy(Duration consumerStall, Duration sweeperStall) {
        long now = System.nanoTime();
        return now - lastConsumerPassNanos <= consumerStall.toNanos()
                && now - lastSweeperPassNanos <= sweeperStall.toNanos();
    }

    /**
     * LIVENESS: true while both loop threads are alive and still being scheduled — the heartbeat
     * is stamped at the start of every iteration, failing ones included, so an infrastructure
     * outage mid-backoff keeps /alive at 200 (restarting the process would not fix the broker)
     * and only a thread that genuinely stopped iterating past the tolerance — or died, reported
     * immediately — turns it 503, for an orchestrator's liveness probe to bounce the process.
     * The tolerance must exceed the longest legitimate gap between iterations: a sweep interval
     * plus a send/flush blocked for up to {@link #DELIVERY_TIMEOUT}/{@link #MAX_BLOCK} (30s)
     * plus one {@link #MAX_BACKOFF} (30s). {@link Main#ALIVE_STALL_FLOOR} computes the floor
     * from these same constants, and Main's default of 120s covers the whole worst-case gap.
     */
    public boolean alive(Duration stallTolerance) {
        if (consumerThread == null || !consumerThread.isAlive()
                || sweeperThread == null || !sweeperThread.isAlive()) {
            return false;   // a dead thread is definitively gone; no threshold to wait out
        }
        long now = System.nanoTime();
        return now - lastConsumerBeatNanos <= stallTolerance.toNanos()
                && now - lastSweeperBeatNanos <= stallTolerance.toNanos();
    }

    private void consume(String bootstrapServers) {
        long backoffMs = INITIAL_BACKOFF_MS;
        // sticky until a rewind SUCCEEDS: a failed pass leaves the in-memory position past the
        // records it lost, so no poll (and above all no commit) may happen before the seek-back
        // lands — otherwise a commit would quietly seal the skipped batch
        boolean rewindNeeded = false;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrapServers))) {
            this.consumer = consumer;
            consumer.subscribe(topics);
            while (running && !Thread.currentThread().isInterrupted()) {
                // the liveness heartbeat: every iteration, including one about to fail or back
                // off — /alive watches scheduling, not success (success is /health's business)
                lastConsumerBeatNanos = System.nanoTime();
                try {
                    if (rewindNeeded) {
                        rewindToCommitted(consumer);   // throws if it cannot; the flag survives
                        rewindNeeded = false;
                    }
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    List<Sent> sent = new ArrayList<>();
                    for (ConsumerRecord<String, String> record : records) {
                        String cid = header(record, CID_HEADER);
                        if (cid != null) {
                            MDC.put("cid", cid);   // continue the trace the deletion request started
                        }
                        try {
                            for (EventsRouter.Outgoing outgoing : router.handle(record.topic(), record.value())) {
                                sent.add(new Sent(outgoing, send(producer, outgoing, cid)));
                            }
                        } finally {
                            MDC.remove("cid");
                        }
                    }
                    producer.flush();            // outcomes on the broker...
                    settleDeliveries(sent);      // ...PROVEN there, and the outbox marked...
                    consumer.commitSync();       // ...and only then does the offset move
                    lastConsumerPassNanos = System.nanoTime();
                    backoffMs = INITIAL_BACKOFF_MS;
                } catch (WakeupException woken) {
                    // the shutdown hook waking a blocked poll; the while condition decides
                } catch (Exception infrastructure) {
                    LOG.error("offboarding consumer pass failed; will rewind to the committed"
                            + " offsets and retry", infrastructure);
                    rewindNeeded = true;
                    backoffMs = pause(backoffMs);   // the backoff covers failed rewinds too
                }
            }
            // the shutdown interrupt has done its job (cutting a poll or a backoff sleep
            // short); clear the flag so the consumer's close() can still drain gracefully
            Thread.interrupted();
        }
        LOG.info("offboarding consumer loop stopped");
    }

    private void sweep() {
        long backoffMs = INITIAL_BACKOFF_MS;
        while (running && !Thread.currentThread().isInterrupted()) {
            // the liveness heartbeat, exactly like the consumer's: iterations, not successes
            lastSweeperBeatNanos = System.nanoTime();
            try {
                Thread.sleep(sweepEvery.toMillis());
                List<Sent> sent = new ArrayList<>();
                for (EventsRouter.Outgoing outgoing : router.sweepOverdue()) {
                    sent.add(new Sent(outgoing, send(producer, outgoing, null)));
                }
                producer.flush();            // same order as the consumer: broker first (proven),
                settleDeliveries(sent);      // outbox second — a failed send stays unannounced
                                             // and the NEXT sweep simply tries again
                lastSweeperPassNanos = System.nanoTime();
                backoffMs = INITIAL_BACKOFF_MS;
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            } catch (Exception infrastructure) {
                // ERROR, matching the consumer's failed pass: an unfinished sweep is the same
                // class of trouble (retries not offered, outcomes not re-announced), not a shrug
                LOG.error("offboarding sweeper pass failed; retrying", infrastructure);
                MetricsEndpoint.sweeperPassFailed();
                backoffMs = pause(backoffMs);
            }
        }
        LOG.info("offboarding sweeper loop stopped");
    }

    /** One outgoing event and the broker's (eventual) word on whether it truly arrived. */
    private record Sent(EventsRouter.Outgoing outgoing, Future<RecordMetadata> delivery) {
    }

    /**
     * The outbox's second half, made honest: flush() pushes the batch out but reports no delivery
     * errors, so every send's future is checked here — after the flush they are already settled,
     * the get() does not really block. Only an outcome the broker demonstrably ACCEPTED earns its
     * announced mark, and only a re-command that demonstrably ARRIVED charges its saga's retry
     * counter — a broker outage burns no retries, so the sweeper can never capitulate without
     * having re-asked on the wire. ANY failure then fails the pass (no commit, the retry path
     * re-delivers), while the sends that did land keep their marks — re-handling is idempotent.
     */
    private void settleDeliveries(List<Sent> sent) throws Exception {
        Exception firstFailure = null;
        for (Sent each : sent) {
            try {
                each.delivery().get();
            } catch (Exception undelivered) {
                if (firstFailure == null) {
                    firstFailure = undelivered;
                }
                continue;   // no announced mark, no counted retry, for what never reached the broker
            }
            if (each.outgoing().announcesSaga() != null) {
                store.markAnnounced(each.outgoing().announcesSaga());
            }
            if (each.outgoing().countsRetryFor() != null
                    && store.retryDelivered(each.outgoing().countsRetryFor())) {
                // metered only when the store actually charged the counter: a delivery landing
                // on a saga that meanwhile finished is a no-op there and must be one here too,
                // or the metric would drift ahead of the sum of retries in the store
                MetricsEndpoint.retryDelivered();
            }
        }
        if (firstFailure != null) {
            throw new IllegalStateException(
                    "at least one outgoing event never reached the broker", firstFailure);
        }
    }

    /**
     * A failed pass must not lose its records: poll() already advanced the in-memory position
     * past them, so seek every assigned partition back to its committed offset (or the beginning,
     * matching auto.offset.reset=earliest) before retrying — otherwise the retry would quietly
     * skip the very batch that failed. A failure HERE propagates: the caller keeps its
     * rewind-needed flag up and must not poll (let alone commit) until a rewind succeeds.
     */
    private static void rewindToCommitted(KafkaConsumer<String, String> consumer) {
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
        for (TopicPartition partition : assignment) {
            OffsetAndMetadata offset = committed.get(partition);
            if (offset == null) {
                consumer.seekToBeginning(List.of(partition));
            } else {
                consumer.seek(partition, offset.offset());
            }
        }
    }

    /** Back off before the retry: one second doubling to thirty, reset by any successful pass. */
    private static long pause(long backoffMs) {
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        }
        return Math.min(backoffMs * 2, MAX_BACKOFF_MS);
    }

    /**
     * The orderly exit, from the JVM's shutdown hook: stop the loops, wake the possibly-blocked
     * poll (wakeup is the consumer's only thread-safe method), wait for both passes to finish,
     * and close the producer last so any in-flight outcome still drains to the broker.
     * Package-visible so the integration test can stop a loop without stopping the JVM.
     */
    void shutdown() {
        running = false;
        if (sweeperThread != null) {
            sweeperThread.interrupt();   // it is usually mid-sleep between sweeps
        }
        KafkaConsumer<String, String> consumer = this.consumer;
        if (consumer != null) {
            consumer.wakeup();
        }
        if (consumerThread != null) {
            // the wakeup only reaches a BLOCKED poll; a consumer mid-backoff is asleep in
            // pause(), and the interrupt is what cuts that sleep short so shutdown never
            // waits out a thirty-second backoff
            consumerThread.interrupt();
        }
        join(consumerThread);
        join(sweeperThread);
        KafkaProducer<String, String> producer = this.producer;
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }

    private static void join(Thread thread) {
        if (thread == null) {
            return;
        }
        try {
            thread.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static Future<RecordMetadata> send(KafkaProducer<String, String> producer,
                                               EventsRouter.Outgoing outgoing, String cid) {
        ProducerRecord<String, String> out =
                new ProducerRecord<>(outgoing.topic(), outgoing.key(), outgoing.payload());
        if (cid != null) {
            out.headers().add(CID_HEADER, cid.getBytes(StandardCharsets.UTF_8));
        }
        return producer.send(out);   // the future is the broker's word; settleDeliveries checks it
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Properties consumerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "offboarding");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // the delivery clocks, EXPLICIT because /alive depends on them (see DELIVERY_TIMEOUT):
        // during a broker outage flush() blocks an iteration for up to delivery.timeout.ms and a
        // metadata-less send() for up to max.block.ms — both must stay well inside the /alive
        // stall tolerance, whose floor Main derives from these very constants. Kafka's defaults
        // (120s / 60s) would let one blocked iteration outlast the probe and turn an outage into
        // a false "dead thread" restart
        props.put("delivery.timeout.ms", String.valueOf(deliveryTimeout.toMillis()));
        props.put("request.timeout.ms", String.valueOf(requestTimeout.toMillis()));
        props.put("max.block.ms", String.valueOf(maxBlock.toMillis()));
        return props;
    }
}
