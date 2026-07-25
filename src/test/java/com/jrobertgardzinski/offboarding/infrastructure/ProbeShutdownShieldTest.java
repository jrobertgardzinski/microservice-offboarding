package com.jrobertgardzinski.offboarding.infrastructure;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The readiness probe must tell a STOPPING service from a SILENT broker. {@link KafkaLoop#shutdown}
 * sends two signals — {@code wakeup()} for a blocked poll and {@code interrupt()} for everything
 * else — and Kafka answers the second with its own {@link InterruptException}. The probe used to
 * shield only the wakeup, so the interrupt fell into the generic catch, came back wrapped as "the
 * broker did not answer", and every ordinary restart logged {@code got no answer from the broker}
 * at ERROR while the broker was perfectly healthy. An operator reading that log learns nothing
 * true, and learns to ignore the line that matters.
 *
 * <p>The mirror image lives in microservice-user-collections, whose consumer is never woken and
 * therefore shields only the interrupt: each probe shields exactly the signals its own shutdown
 * can deliver, which is what "symmetric" means here.
 */
class ProbeShutdownShieldTest {

    private final RouterFixture fixture = RouterFixture.router();
    private final KafkaLoop loop = new KafkaLoop(
            fixture.router, fixture.store, List.of("security-events"), Duration.ofSeconds(15));

    @AfterEach
    void clearTheInterruptKafkaSet() {
        // InterruptException's constructor interrupts the current thread; left standing, the flag
        // would leak into whatever test JUnit runs next on this thread
        Thread.interrupted();
    }

    @Test
    void a_shutdown_interrupt_rides_through_the_probe_untouched() {
        // the finding: this used to come back as BrokerSilent, and consume()'s catch logged
        // "offboarding consumer got no answer from the broker" at ERROR on every clean stop
        InterruptException stopping = new InterruptException("shutting down");
        InterruptException thrown = assertThrows(InterruptException.class,
                () -> loop.probeBroker(consumerThrowing(() -> stopping)));
        assertSame(stopping, thrown, "the stop must arrive as itself, not as a broker verdict");
    }

    @Test
    void a_shutdown_wakeup_rides_through_the_probe_untouched() {
        WakeupException woken = new WakeupException();
        WakeupException thrown = assertThrows(WakeupException.class,
                () -> loop.probeBroker(consumerThrowing(() -> woken)));
        assertSame(woken, thrown, "the stop must arrive as itself, not as a broker verdict");
    }

    @Test
    void a_broker_that_really_does_not_answer_still_becomes_a_broker_verdict() {
        // the shields must not swallow the case the probe exists for
        IllegalStateException silence = new IllegalStateException("no reply");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> loop.probeBroker(consumerThrowing(() -> silence)));
        assertTrue(thrown.getMessage().contains("did not answer the readiness probe"),
                "a genuine silence must still read as one: " + thrown.getMessage());
        assertSame(silence, thrown.getCause(), "with the original failure kept as the cause");
    }

    /** A consumer whose metadata round trip fails with whatever the test hands it. */
    private static MockConsumer<String, String> consumerThrowing(
            Supplier<RuntimeException> failure) {
        return new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
                throw failure.get();
            }
        };
    }
}
