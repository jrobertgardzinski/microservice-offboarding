package com.jrobertgardzinski.offboarding.infrastructure;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Main's boot-time configuration hygiene: a mangled numeric env refuses to boot with a message
 * that names the variable (not a bare NumberFormatException), the sweeper's stall tolerance is
 * floored at the sweep interval — below it /health would call a healthy sweeper stalled — and the
 * participant spec may not name a participant or a topic twice, because either repeat shrinks the
 * set of confirmations the saga waits for and buys a premature PORTAL_CONTENT_PURGED.
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

    @Test
    void the_shipped_participant_spec_parses_into_one_topic_per_participant() {
        Map<String, String> byTopic = Main.parseParticipants(Main.DEFAULT_PARTICIPANTS);
        assertEquals(Map.of("memes-events", "memes",
                        "comments-events", "comments",
                        "usercollections-events", "collections"), byTopic,
                "the default spec is the contract the three content services publish on");
        assertEquals(3, Set.copyOf(byTopic.values()).size(),
                "and it must survive the values()->Set collapse main() does: three topics, three"
                        + " DISTINCT names, three confirmations to wait for");
    }

    @Test
    void a_participant_entry_that_is_not_name_equals_topic_refuses_to_boot() {
        for (String nonsense : new String[]{"memes", "memes=", "=memes-events", "memes= "}) {
            assertThrows(IllegalArgumentException.class, () -> Main.parseParticipants(nonsense),
                    "must refuse the entry \"" + nonsense + "\"");
        }
    }

    @Test
    void a_repeated_participant_NAME_refuses_to_boot_instead_of_shrinking_the_quorum() {
        // THE finding this pins. main() derives the set of confirmations to wait for from
        // participantByTopic.values(), so "memes" twice on two topics collapses to ONE required
        // confirmation while THREE services still hold the leaver's content. The saga would then
        // announce PORTAL_CONTENT_PURGED after two confirmations instead of three, security would
        // delete the account for good, and the participant nobody waited for would have neither a
        // timeout nor a compensation left to report its own failure. Data gone, verdict false.
        String typo = "memes=memes-events,memes=other-events,comments=comments-events";
        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> Main.parseParticipants(typo));
        assertTrue(refusal.getMessage().contains(Main.PARTICIPANTS_ENV),
                "the refusal must name the variable to fix: " + refusal.getMessage());
        assertTrue(refusal.getMessage().contains("memes"),
                "and the participant it saw twice: " + refusal.getMessage());
        assertTrue(refusal.getMessage().contains("memes-events")
                        && refusal.getMessage().contains("other-events"),
                "and BOTH topics it saw it on, so the operator knows which line to delete: "
                        + refusal.getMessage());
    }

    @Test
    void a_repeated_TOPIC_refuses_to_boot_instead_of_dropping_a_participant() {
        // the quieter half of the same typo: two names on one topic used to overwrite the earlier
        // entry, so that participant simply stopped being subscribed to — no confirmation could
        // ever arrive from it, and the saga would burn its retries and capitulate for no reason
        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> Main.parseParticipants("memes=shared-events,comments=shared-events"));
        assertTrue(refusal.getMessage().contains(Main.PARTICIPANTS_ENV),
                "the refusal must name the variable: " + refusal.getMessage());
        assertTrue(refusal.getMessage().contains("shared-events"),
                "and echo the topic it saw twice: " + refusal.getMessage());
        assertTrue(refusal.getMessage().contains("memes")
                        && refusal.getMessage().contains("comments"),
                "and both participants that claimed it: " + refusal.getMessage());

        // an entry repeated verbatim is the same refusal: config that says a thing twice is config
        // whose author lost track, not an invitation to guess which copy was meant
        assertThrows(IllegalArgumentException.class,
                () -> Main.parseParticipants("memes=memes-events,memes=memes-events"));
    }

    @Test
    void one_participant_may_be_named_after_another_participants_topic_prefix() {
        // the checks must be about EQUALITY, not about looking alike: a "memes" participant and a
        // "memes-archive" participant on "memes-archive-events" is a perfectly good deployment,
        // and a validation that rejected it would block the very extension the config exists for
        Map<String, String> byTopic = Main.parseParticipants(
                "memes=memes-events,memes-archive=memes-archive-events");
        assertEquals(Map.of("memes-events", "memes",
                "memes-archive-events", "memes-archive"), byTopic);
    }

    @Test
    void whitespace_around_the_pairs_is_forgiven_and_an_empty_spec_stays_legal() {
        assertEquals(Map.of("memes-events", "memes", "comments-events", "comments"),
                Main.parseParticipants(" memes = memes-events , comments = comments-events "),
                "a spec pasted from a YAML manifest carries spaces; they are not a typo");
        assertEquals(Map.of(), Main.parseParticipants(""),
                "and an empty spec still means what the code says it means: no content"
                        + " participants at all (dev, tests) — not a refusal");
    }
}
