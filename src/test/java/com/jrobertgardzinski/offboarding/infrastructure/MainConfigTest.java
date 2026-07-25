package com.jrobertgardzinski.offboarding.infrastructure;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Main's boot-time configuration hygiene: a mangled numeric env refuses to boot with a message
 * that names the variable (not a bare NumberFormatException), and the sweeper's stall tolerance
 * is floored at the sweep interval — below it /health would call a healthy sweeper stalled.
 */
class MainConfigTest {

    @Test
    void an_absent_or_blank_numeric_env_falls_back_to_the_default() {
        assertEquals(120, Main.parseLongOrRefuse("OFFBOARDING_PURGE_TIMEOUT_SEC", null, 120));
        assertEquals(120, Main.parseLongOrRefuse("OFFBOARDING_PURGE_TIMEOUT_SEC", "  ", 120));
    }

    @Test
    void a_parsable_numeric_env_wins_over_the_default() {
        assertEquals(45, Main.parseLongOrRefuse("OFFBOARDING_CONSUMER_STALL_SEC", " 45 ", 60));
    }

    @Test
    void a_mangled_numeric_env_refuses_to_boot_naming_the_variable() {
        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> Main.parseLongOrRefuse("OFFBOARDING_PORT", "80 94", 8094));
        assertTrue(refusal.getMessage().contains("OFFBOARDING_PORT"),
                "the refusal must name the variable to fix: " + refusal.getMessage());
        assertTrue(refusal.getMessage().contains("80 94"),
                "and echo the value it choked on: " + refusal.getMessage());
    }

    @Test
    void a_value_inside_its_range_passes_through() {
        assertEquals(8094, Main.inRangeOrRefuse("OFFBOARDING_PORT", 8094, 1, 65535));
        assertEquals(0, Main.inRangeOrRefuse("OFFBOARDING_MAX_PURGE_RETRIES", 0, 0, 100));
    }

    @Test
    void a_port_outside_1_to_65535_refuses_to_boot_naming_variable_and_value() {
        for (long nonsense : new long[]{0, -1, 65536}) {
            IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                    () -> Main.inRangeOrRefuse("OFFBOARDING_PORT", nonsense, 1, 65535));
            assertTrue(refusal.getMessage().contains("OFFBOARDING_PORT"),
                    "the refusal must name the variable: " + refusal.getMessage());
            assertTrue(refusal.getMessage().contains(String.valueOf(nonsense)),
                    "and echo the value it refused: " + refusal.getMessage());
            assertTrue(refusal.getMessage().contains("65535"),
                    "and state the legal range: " + refusal.getMessage());
        }
    }

    @Test
    void a_retry_budget_outside_0_to_100_refuses_to_boot() {
        assertThrows(IllegalArgumentException.class,
                () -> Main.inRangeOrRefuse("OFFBOARDING_MAX_PURGE_RETRIES", -1, 0, 100));
        assertThrows(IllegalArgumentException.class,
                () -> Main.inRangeOrRefuse("OFFBOARDING_MAX_PURGE_RETRIES", 101, 0, 100));
    }

    @Test
    void a_non_positive_timeout_refuses_to_boot_without_reciting_long_max() {
        // an unbounded maximum must read "at least 1", not "between 1 and 9223372036854775807"
        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> Main.inRangeOrRefuse("OFFBOARDING_PURGE_TIMEOUT_SEC", 0, 1, Long.MAX_VALUE));
        assertTrue(refusal.getMessage().contains("OFFBOARDING_PURGE_TIMEOUT_SEC"));
        assertTrue(refusal.getMessage().contains("at least 1"),
                "the refusal must state the floor readably: " + refusal.getMessage());
    }

    @Test
    void a_stall_below_the_sweep_interval_is_floored_to_the_interval() {
        assertEquals(Main.SWEEP_EVERY, Main.flooredStall("OFFBOARDING_SWEEPER_STALL_SEC",
                Duration.ofSeconds(1), Main.SWEEP_EVERY));
        assertEquals(Main.SWEEP_EVERY, Main.flooredStall("OFFBOARDING_ALIVE_STALL_SEC",
                Duration.ofSeconds(1), Main.SWEEP_EVERY));
    }

    @Test
    void a_stall_at_or_above_the_interval_is_kept() {
        assertEquals(Duration.ofSeconds(60), Main.flooredStall("OFFBOARDING_SWEEPER_STALL_SEC",
                Duration.ofSeconds(60), Main.SWEEP_EVERY));
        assertEquals(Main.SWEEP_EVERY, Main.flooredStall("OFFBOARDING_SWEEPER_STALL_SEC",
                Main.SWEEP_EVERY, Main.SWEEP_EVERY));
    }

    @Test
    void the_alive_floor_is_derived_from_the_loop_clocks_and_the_backoff() {
        // never a magic number: 2 x max(delivery.timeout, default.api.timeout, max backoff) +
        // one sweep interval + one broker probe, plus the margin — recomputed here from the same
        // constants so a drift on either side breaks the build
        Duration longestBlock = longest(KafkaLoop.DELIVERY_TIMEOUT, KafkaLoop.API_TIMEOUT,
                KafkaLoop.MAX_BACKOFF);
        Duration derived = longestBlock.multipliedBy(2)
                .plus(Main.SWEEP_EVERY)
                .plus(KafkaLoop.PROBE_TIMEOUT);
        assertEquals(Duration.ofSeconds(Math.ceilDiv(
                        derived.toMillis() * (100 + Main.FLOOR_MARGIN_PERCENT) / 100, 1_000)),
                Main.ALIVE_STALL_FLOOR);
        assertTrue(Main.ALIVE_STALL_FLOOR.compareTo(derived) > 0,
                "the floor must sit strictly ABOVE the bare arithmetic: alive() compares with"
                        + " <=, and a GC pause on top of an honest worst case must not read dead");
        assertTrue(Main.ALIVE_STALL_FLOOR.compareTo(Duration.ofSeconds(120)) < 0,
                "the documented 120s default must sit above the floor");
    }

    @Test
    void the_consumers_own_blocking_clock_is_explicit_and_inside_the_alive_floor() {
        // the finding this pins: only the PRODUCER's clocks used to be set, so commitSync(),
        // the rewind's committed() lookup and the readiness probe each still waited Kafka's 60s
        // default.api.timeout.ms on a dead broker — one of those plus one max backoff already
        // outlasts the old 75s floor, and a plain outage would read as a wedged thread
        assertEquals(String.valueOf(KafkaLoop.API_TIMEOUT.toMillis()),
                KafkaLoop.consumerProps("localhost:9092").getProperty("default.api.timeout.ms"),
                "the consumer's api timeout must be set explicitly, never left at Kafka's 60s");
        assertEquals(String.valueOf(KafkaLoop.REQUEST_TIMEOUT.toMillis()),
                KafkaLoop.consumerProps("localhost:9092").getProperty("request.timeout.ms"));
        assertTrue(KafkaLoop.REQUEST_TIMEOUT.compareTo(KafkaLoop.API_TIMEOUT) < 0,
                "one in-flight request must fit inside one API call");
        assertTrue(Main.ALIVE_STALL_FLOOR.compareTo(
                        KafkaLoop.API_TIMEOUT.multipliedBy(2).plus(KafkaLoop.MAX_BACKOFF)) > 0,
                "two consumer API waits and a backoff must still fit inside the tolerance");
    }

    private static Duration longest(Duration first, Duration... rest) {
        Duration longest = first;
        for (Duration candidate : rest) {
            if (candidate.compareTo(longest) > 0) {
                longest = candidate;
            }
        }
        return longest;
    }

    @Test
    void an_alive_stall_below_the_derived_floor_is_floored() {
        // a tolerance below the floor would let a broker outage (send/flush legitimately blocked
        // up to delivery.timeout, then the backoff) read as a dead thread and restart the pod
        assertEquals(Main.ALIVE_STALL_FLOOR, Main.flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Duration.ofSeconds(30)));
        assertEquals(Main.ALIVE_STALL_FLOOR, Main.flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Main.ALIVE_STALL_FLOOR.minusSeconds(1)));
    }

    @Test
    void an_alive_stall_at_or_above_the_derived_floor_is_kept() {
        assertEquals(Duration.ofSeconds(120), Main.flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Duration.ofSeconds(120)));
        assertEquals(Main.ALIVE_STALL_FLOOR, Main.flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Main.ALIVE_STALL_FLOOR));
    }
}
