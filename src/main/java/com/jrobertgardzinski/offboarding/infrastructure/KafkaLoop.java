package com.jrobertgardzinski.offboarding.infrastructure;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * The real transport around the pure {@link EventsRouter}: one consumer polls the deletion fact
 * and every participant topic, the shared (thread-safe) producer publishes whatever the router
 * answers plus the sweeper's failure announcements. The correlation id rides the Kafka header, in
 * and out, so the async hops keep the trace of the request that started the deletion. Offsets
 * commit after the answers are sent — at-least-once, which the idempotent saga transitions absorb.
 */
public class KafkaLoop {

    static final String CID_HEADER = "X-Correlation-Id";

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLoop.class);

    private final EventsRouter router;
    private final Collection<String> topics;
    private final Duration sweepEvery;

    public KafkaLoop(EventsRouter router, Collection<String> topics, Duration sweepEvery) {
        this.router = router;
        this.topics = topics;
        this.sweepEvery = sweepEvery;
    }

    /** Starts the consuming loop and the timeout sweeper, each on its own daemon virtual thread. */
    public void start(String bootstrapServers) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrapServers));
        Thread.ofVirtual().name("offboarding-consumer").start(() -> consume(bootstrapServers, producer));
        Thread.ofVirtual().name("offboarding-sweeper").start(() -> sweep(producer));
    }

    private void consume(String bootstrapServers, KafkaProducer<String, String> producer) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrapServers))) {
            consumer.subscribe(topics);
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    String cid = header(record, CID_HEADER);
                    if (cid != null) {
                        MDC.put("cid", cid);   // continue the trace the deletion request started
                    }
                    try {
                        for (EventsRouter.Outgoing outgoing : router.handle(record.topic(), record.value())) {
                            send(producer, outgoing, cid);
                        }
                    } finally {
                        MDC.remove("cid");
                    }
                }
                consumer.commitSync();
            }
        } catch (Exception broken) {
            LOG.warn("offboarding consumer stopped: " + broken.getMessage());
        }
    }

    private void sweep(KafkaProducer<String, String> producer) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(sweepEvery.toMillis());
                for (EventsRouter.Outgoing outgoing : router.sweepOverdue()) {
                    send(producer, outgoing, null);
                }
            }
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
        } catch (Exception broken) {
            LOG.warn("offboarding sweeper stopped: " + broken.getMessage());
        }
    }

    private static void send(KafkaProducer<String, String> producer, EventsRouter.Outgoing outgoing,
                             String cid) {
        ProducerRecord<String, String> out =
                new ProducerRecord<>(outgoing.topic(), outgoing.key(), outgoing.payload());
        if (cid != null) {
            out.headers().add(CID_HEADER, cid.getBytes(StandardCharsets.UTF_8));
        }
        producer.send(out);
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
