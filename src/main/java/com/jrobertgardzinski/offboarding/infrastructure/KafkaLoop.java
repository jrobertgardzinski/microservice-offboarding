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
 * thirty — and retries; the loops themselves never die, and each successful pass stamps a
 * liveness flag that /health watches. Within a pass the order is the at-least-once guarantee:
 * outcomes DEMONSTRABLY reach the broker (flush, then every send's future checked — flush alone
 * reports no delivery errors), then the outbox marks them announced, then — and only then — the
 * offset commits. Any failed send fails the whole pass: nothing announced for it, no offsets
 * committed, the rewind-and-retry path picks the batch up again. A crash between any two steps
 * re-delivers or re-publishes, and the idempotent saga transitions and consumers absorb the
 * duplicates.
 */
public class KafkaLoop {

    static final String CID_HEADER = "X-Correlation-Id";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLoop.class);
    private static final long INITIAL_BACKOFF_MS = 1_000;
    private static final long MAX_BACKOFF_MS = 30_000;

    private final EventsRouter router;
    private final SagaStore store;
    private final Collection<String> topics;
    private final Duration sweepEvery;

    private volatile boolean running = true;
    private volatile KafkaConsumer<String, String> consumer;
    private volatile KafkaProducer<String, String> producer;
    // the liveness flags: /health answers 503 once a loop stops completing passes (see healthy())
    private volatile long lastConsumerPassMillis;
    private volatile long lastSweeperPassMillis;
    private Thread consumerThread;
    private Thread sweeperThread;
    private final AtomicBoolean started = new AtomicBoolean();

    public KafkaLoop(EventsRouter router, SagaStore store, Collection<String> topics,
                     Duration sweepEvery) {
        this.router = router;
        this.store = store;
        this.topics = topics;
        this.sweepEvery = sweepEvery;
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
        // count liveness from here, so a service still warming up is not born unhealthy
        lastConsumerPassMillis = System.currentTimeMillis();
        lastSweeperPassMillis = System.currentTimeMillis();
        producer = new KafkaProducer<>(producerProps(bootstrapServers));
        consumerThread = Thread.ofVirtual().name("offboarding-consumer")
                .start(() -> consume(bootstrapServers));
        sweeperThread = Thread.ofVirtual().name("offboarding-sweeper").start(this::sweep);
        // the started CAS above also makes this exactly one hook per instance — several would
        // run shutdown() over each other and leak threads waiting on the same loop
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "offboarding-loop-shutdown"));
    }

    /** True while both loops keep completing passes within their stall tolerances. */
    public boolean healthy(Duration consumerStall, Duration sweeperStall) {
        long now = System.currentTimeMillis();
        return now - lastConsumerPassMillis <= consumerStall.toMillis()
                && now - lastSweeperPassMillis <= sweeperStall.toMillis();
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
                    lastConsumerPassMillis = System.currentTimeMillis();
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
            try {
                Thread.sleep(sweepEvery.toMillis());
                List<Sent> sent = new ArrayList<>();
                for (EventsRouter.Outgoing outgoing : router.sweepOverdue()) {
                    sent.add(new Sent(outgoing, send(producer, outgoing, null)));
                }
                producer.flush();            // same order as the consumer: broker first (proven),
                settleDeliveries(sent);      // outbox second — a failed send stays unannounced
                                             // and the NEXT sweep simply tries again
                lastSweeperPassMillis = System.currentTimeMillis();
                backoffMs = INITIAL_BACKOFF_MS;
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            } catch (Exception infrastructure) {
                LOG.warn("offboarding sweeper pass failed; retrying", infrastructure);
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
            if (each.outgoing().countsRetryFor() != null) {
                store.retryDelivered(each.outgoing().countsRetryFor());
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

    private static Properties producerProps(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
