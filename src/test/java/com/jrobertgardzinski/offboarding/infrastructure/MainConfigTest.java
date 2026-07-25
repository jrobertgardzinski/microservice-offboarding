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
    void the_alive_floor_covers_the_whole_worst_legal_iteration() {
        // The finding this pins. The floor used to be 2 x max(delivery.timeout, api.timeout,
        // max backoff) + a sweep + a probe = 100s, and the test recomputed that same formula —
        // which passes no matter how wrong the formula is. A worst legal iteration actually
        // spends its blocks in SEQUENCE, and back then they came to 106s: the "safe minimum" sat
        // BELOW the case it was sold as covering. So the blocks are enumerated here from what
        // one pass really calls, in order, and the floor must COVER their sum.
        Duration worstIteration = KafkaLoop.API_TIMEOUT      // rewindToCommitted(): committed()
                .plus(KafkaLoop.PROBE_TIMEOUT)               // the /health honesty probe
                .plus(KafkaLoop.POLL_TIMEOUT)                // poll()
                .plus(Database.WORST_BLOCK)                  // the saga store, via router.handle
                .plus(KafkaLoop.DELIVERY_TIMEOUT)            // producer.flush()
                .plus(KafkaLoop.API_TIMEOUT)                 // commitSync()
                .plus(KafkaLoop.MAX_BACKOFF);                // pause() before the retry
        assertEquals(worstIteration, Main.CONSUMER_WORST_ITERATION,
                "Main must add up the blocks a pass really spends, in full");
        assertTrue(Main.ALIVE_STALL_FLOOR.compareTo(worstIteration) > 0,
                "the floor must sit strictly ABOVE the worst legal iteration: alive() compares"
                        + " with <=, and a GC pause on top of an honest worst case must not read"
                        + " dead. Floor " + Main.ALIVE_STALL_FLOOR.toSeconds() + "s vs iteration "
                        + worstIteration.toSeconds() + "s");
        assertTrue(Main.ALIVE_STALL_FLOOR.compareTo(Main.SWEEPER_WORST_ITERATION) > 0,
                "and above the OTHER loop's worst iteration too — both threads share one"
                        + " tolerance, so the floor must cover whichever is worse");

        // and the absolute values, spelled out: a silent drift in any constant above (or in the
        // margin) has to break the build with the new number visible, not slide through
        assertEquals(Duration.ofSeconds(146), Main.CONSUMER_WORST_ITERATION,
                "20 (committed) + 5 (probe) + 1 (poll) + 40 (database) + 30 (flush)"
                        + " + 20 (commit) + 30 (backoff)");
        assertEquals(Duration.ofSeconds(115), Main.SWEEPER_WORST_ITERATION,
                "15 (sweep interval) + 40 (database) + 30 (flush) + 30 (backoff)");
        assertEquals(Duration.ofSeconds(183), Main.ALIVE_STALL_FLOOR, "146s + 25% margin");
    }

    @Test
    void the_code_default_sits_above_the_floor_instead_of_being_corrected_by_it() {
        // a default the floor silently raises is not a default: the javadoc, the k8s manifests
        // and the operator would all be quoting a number the service never uses. 120s stopped
        // being one the moment the floor was computed honestly (183s)
        assertTrue(Main.DEFAULT_ALIVE_STALL.compareTo(Main.ALIVE_STALL_FLOOR) >= 0,
                "OFFBOARDING_ALIVE_STALL_SEC's default (" + Main.DEFAULT_ALIVE_STALL.toSeconds()
                        + "s) must not be below the floor (" + Main.ALIVE_STALL_FLOOR.toSeconds()
                        + "s)");
        assertEquals(Main.DEFAULT_ALIVE_STALL,
                Main.flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC", Main.DEFAULT_ALIVE_STALL),
                "and it must therefore pass through the floor untouched");
    }

    @Test
    void the_database_clocks_are_a_term_of_the_floor_not_an_unbounded_wait() {
        // the last unguarded block in the loop: pgjdbc leaves socketTimeout at 0 = forever, so a
        // SILENT database (partition, frozen node, a DELETE behind somebody else's lock) used to
        // wedge the loop thread with no bound at all — no beat, /alive 503, restart, same lock
        assertTrue(Database.SOCKET_TIMEOUT.toSeconds() > 0,
                "an unbounded socket read is an unbounded liveness gap");
        assertTrue(Database.STATEMENT_TIMEOUT.compareTo(Database.SOCKET_TIMEOUT) < 0,
                "the server-side cancel must fire BEFORE the client abandons the socket, or the"
                        + " lock waiter outlives the connection that was waiting on it");
        assertEquals(Database.CONNECTION_TIMEOUT.plus(Database.SOCKET_TIMEOUT),
                Database.WORST_BLOCK,
                "the two chain in the worst case: a near-full wait for a connection, then a"
                        + " silent read on it");
        assertTrue(Main.CONSUMER_WORST_ITERATION.compareTo(Database.WORST_BLOCK) > 0,
                "and the floor's arithmetic must actually carry that block");
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
        assertEquals(Duration.ofSeconds(300), Main.flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Duration.ofSeconds(300)));
        assertEquals(Main.ALIVE_STALL_FLOOR, Main.flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Main.ALIVE_STALL_FLOOR));
    }
}
