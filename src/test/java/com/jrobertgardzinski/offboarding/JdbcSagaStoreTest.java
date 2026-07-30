package com.jrobertgardzinski.offboarding;

import com.jrobertgardzinski.offboarding.application.SagaStore;
import com.jrobertgardzinski.offboarding.infrastructure.Database;
import com.jrobertgardzinski.offboarding.infrastructure.JdbcSagaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JDBC adapter on the same H2 (PostgreSQL mode) the dev profile runs — the same Flyway
 * migrations as production, no second schema to drift. Exercises what the in-memory double
 * mirrors: the running-saga lookup, the once-latch on completion, the retry-then-compensate
 * sweep, the outbox flag, and the retention window — plus the V2 UNIQUE constraints under
 * genuine thread races.
 */
class JdbcSagaStoreTest {

    private static final Set<String> THREE = Set.of("memes", "comments", "collections");
    private static final Instant T0 = Instant.parse("2026-07-11T12:00:00Z");
    /** maxRetries=0: compensate on the first overdue sweep, the pre-retry behaviour. */
    private static final int NO_RETRIES = 0;
    /** The deployed purge timeout (OFFBOARDING_PURGE_TIMEOUT_SEC, 120s) — the sweep's window. */
    private static final long TIMEOUT = 120;

    private final DataSource dataSource = Database.migratedDataSource();
    private final JdbcSagaStore store = new JdbcSagaStore(dataSource);

    @BeforeEach
    void cleanSlate() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM offboarding_confirmations");
            statement.executeUpdate("DELETE FROM offboarding_sagas");
        }
    }

    @Test
    void a_replayed_fact_finds_its_saga_even_after_completion() {
        UUID fact = UUID.randomUUID();
        UUID first = store.start(fact, "alice@example.com", T0);
        store.complete("alice@example.com", T0);
        UUID replayed = store.start(fact, "alice@example.com", T0.plusSeconds(5));
        assertEquals(first, replayed, "a replayed fact must not fork a second saga");
    }

    @Test
    void a_second_request_while_one_runs_joins_the_running_saga() {
        UUID first = store.start(UUID.randomUUID(), "alice@example.com", T0);
        UUID second = store.start(UUID.randomUUID(), "alice@example.com", T0.plusSeconds(5));
        assertEquals(first, second, "one running saga per account");
    }

    @Test
    void racing_starts_with_the_same_fact_agree_on_one_saga() throws Exception {
        // both threads pass the read-before-insert together; the fact_id UNIQUE turns one insert
        // into a 23505 and the loser must adopt the winner's saga instead of failing
        UUID fact = UUID.randomUUID();
        List<UUID> sagas = race(
                () -> store.start(fact, "race@example.com", T0),
                () -> store.start(fact, "race@example.com", T0));
        assertEquals(sagas.get(0), sagas.get(1), "a replayed fact must not fork under a race either");
    }

    @Test
    void racing_starts_for_the_same_email_agree_on_one_saga() throws Exception {
        // different facts, same account: the running_email UNIQUE (V2) is what makes "one running
        // saga per email" hold even when the application-level check races
        List<UUID> sagas = race(
                () -> store.start(UUID.randomUUID(), "race@example.com", T0),
                () -> store.start(UUID.randomUUID(), "race@example.com", T0));
        assertEquals(sagas.get(0), sagas.get(1), "two facts must not fork two sagas for one email");
    }

    private List<UUID> race(java.util.concurrent.Callable<UUID> left,
                            java.util.concurrent.Callable<UUID> right) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<UUID> a = pool.submit(() -> {
                barrier.await();
                return left.call();
            });
            Future<UUID> b = pool.submit(() -> {
                barrier.await();
                return right.call();
            });
            return List.of(a.get(), b.get());
        }
    }

    @Test
    void a_second_started_row_for_one_email_is_rejected_by_the_database_itself() throws Exception {
        // belt and braces: even code that skips the adapter cannot fork a running saga
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement insert = connection.prepareStatement(
                     "INSERT INTO offboarding_sagas "
                             + "(id, fact_id, email, running_email, state, created_at, updated_at) "
                             + "VALUES (?, ?, 'alice@example.com', 'alice@example.com', 'STARTED', ?, ?)")) {
            insert.setObject(1, UUID.randomUUID());
            insert.setObject(2, UUID.randomUUID());
            insert.setTimestamp(3, Timestamp.from(T0));
            insert.setTimestamp(4, Timestamp.from(T0));
            SQLException rejected = assertThrows(SQLException.class, insert::executeUpdate);
            assertEquals("23505", rejected.getSQLState(), "the running_email UNIQUE must fire");
        }
    }

    @Test
    void only_the_last_required_confirmation_completes_and_only_once() {
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        assertFalse(completes(store.confirm("alice@example.com", null, "memes", THREE, T0)));
        assertFalse(completes(store.confirm("alice@example.com", null, "memes", THREE, T0)),
                "duplicate is a no-op");
        assertFalse(completes(store.confirm("alice@example.com", null, "comments", THREE, T0)));
        assertTrue(completes(store.confirm("alice@example.com", null, "collections", THREE, T0)),
                "the last one completes");
        assertTrue(store.confirm("alice@example.com", null, "collections", THREE, T0).isEmpty(),
                "the once-latch: completion is reported to exactly one caller — and the saga is"
                        + " no longer STARTED, so this echo is a stray that records nothing");
    }

    /** Did this confirmation complete the saga? Empty means a stray: it landed nowhere. */
    private static boolean completes(java.util.Optional<SagaStore.Recorded> landed) {
        return landed.map(SagaStore.Recorded::completedSaga).orElse(false);
    }

    @Test
    void a_recorded_confirmation_is_distinguishable_from_a_stray() {
        // the two used to be one answer (Optional.empty) and the router logged both as "recorded
        // ... not complete yet", telling an operator that a dropped confirmation had been stored
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        SagaStore.Recorded recorded =
                store.confirm("alice@example.com", null, "memes", THREE, T0).orElseThrow();
        assertFalse(recorded.completedSaga(), "recorded, and the saga still owes two");
        assertTrue(store.confirm("nobody@example.com", null, "memes", THREE, T0).isEmpty(),
                "a stray landed on nothing at all — a different event, and a different log line");
    }

    @Test
    void a_confirmation_addressed_by_saga_id_lands_on_that_saga() {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        assertEquals(saga,
                store.confirm("alice@example.com", saga, "memes", Set.of("memes"), T0)
                        .orElseThrow().sagaId(),
                "the echoed saga id is the precise address");
    }

    @Test
    void the_quorum_recorded_at_start_survives_a_reconfiguration() {
        // the saga opened when three participants were configured; the operator then shortened
        // OFFBOARDING_PARTICIPANTS and restarted. Reading the CURRENT configuration on every
        // confirmation would close this case on the first answer — announcing PORTAL_CONTENT_PURGED
        // while the content of two services the saga did command is still there
        UUID saga = store.start(new SagaStore.Opening(UUID.randomUUID(), "alice@example.com",
                null, null, THREE), T0);
        assertFalse(completes(store.confirm("alice@example.com", saga, "memes", Set.of("memes"), T0)),
                "the quorum is the one this saga opened with, not the one configured now");
        assertFalse(completes(store.confirm("alice@example.com", saga, "comments", Set.of("memes"), T0)));
        assertTrue(completes(store.confirm("alice@example.com", saga, "collections", Set.of("memes"), T0)),
                "and it completes when THAT quorum is reached");
    }

    @Test
    void a_lengthened_participant_list_does_not_hold_an_open_saga_hostage() {
        // the mirror image: the saga was commanded to ONE participant, and a later configuration
        // added two. Waiting for services that never received a command would make this deletion
        // fail on a timeout instead of completing
        UUID saga = store.start(new SagaStore.Opening(UUID.randomUUID(), "alice@example.com",
                null, null, Set.of("memes")), T0);
        assertTrue(completes(store.confirm("alice@example.com", saga, "memes", THREE, T0)),
                "the only participant that was ever asked is the whole quorum");
    }

    @Test
    void a_saga_that_recorded_no_quorum_falls_back_to_the_configured_one() {
        // every row from before the column existed: the configured set is the honest answer
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        assertFalse(completes(store.confirm("alice@example.com", saga, "memes", THREE, T0)));
        store.confirm("alice@example.com", saga, "comments", THREE, T0);
        assertTrue(completes(store.confirm("alice@example.com", saga, "collections", THREE, T0)));
    }

    @Test
    void the_security_handle_is_stored_at_start_and_handed_back_with_every_verdict() {
        // the correlation the verdict echoes: without it security matches verdicts by email
        // address, and a late verdict of a closed case settles a NEWER deletion for the same person
        UUID securitySaga = UUID.randomUUID();
        UUID saga = store.start(new SagaStore.Opening(UUID.randomUUID(), "alice@example.com",
                null, securitySaga, Set.of("memes", "comments")), T0);

        assertEquals(securitySaga,
                store.confirm("alice@example.com", saga, "memes", THREE, T0)
                        .orElseThrow().securitySagaId(),
                "a confirmation hands back the handle the (possible) verdict must echo");

        SagaStore.SweepResult swept =
                store.sweepOverdue(T0.plusSeconds(TIMEOUT), NO_RETRIES, T0.plusSeconds(TIMEOUT + 1));
        assertEquals(List.of(new SagaStore.Compensated(saga, "alice@example.com", Set.of("memes"),
                        securitySaga)), swept.compensated(),
                "the failure verdict carries the handle of the deletion it is about");
        assertEquals(securitySaga,
                store.unannouncedOutcomes(T0.plusSeconds(9999)).get(0).securitySagaId(),
                "and so does the re-published one — a lost verdict may not lose its address");
    }

    @Test
    void a_second_fact_hands_the_running_saga_the_newer_security_handle() {
        // security only announces a second deletion once its previous saga finished, so the older
        // handle names a closed case: a verdict echoing it would land nowhere and the newer
        // request would hang until security's own timeout
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        UUID saga = store.start(new SagaStore.Opening(UUID.randomUUID(), "alice@example.com",
                null, first, THREE), T0);
        assertEquals(saga, store.start(new SagaStore.Opening(UUID.randomUUID(), "alice@example.com",
                null, second, THREE), T0.plusSeconds(5)), "one running saga per account");
        assertEquals(second,
                store.confirm("alice@example.com", saga, "memes", THREE, T0).orElseThrow()
                        .securitySagaId(),
                "the verdict must reach the request that is actually waiting for it");
    }

    @Test
    void a_confirmation_echoing_a_finished_saga_is_a_stray_and_never_touches_a_newer_one() {
        UUID finished = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.complete("alice@example.com", T0);
        assertTrue(store.confirm("alice@example.com", finished, "memes", THREE, T0).isEmpty(),
                "an echo of a finished saga is a stray, recorded nowhere");
        UUID second = store.start(UUID.randomUUID(), "alice@example.com", T0.plusSeconds(10));
        assertTrue(store.confirm("alice@example.com", finished, "memes", Set.of("memes"),
                        T0.plusSeconds(11)).isEmpty(),
                "even with a NEW saga running for the email, the stale id must stay a stray — "
                        + "falling back to the email lookup would let a closed case confirm the new one");
        assertEquals(new SagaStore.Recorded(second, null, true),
                store.confirm("alice@example.com", second, "memes", Set.of("memes"), T0.plusSeconds(12))
                        .orElseThrow(),
                "the new saga still completes on its OWN confirmation — the stray left no trace");
    }

    @Test
    void a_stray_confirmation_records_nothing() {
        assertTrue(store.confirm("nobody@example.com", null, "memes", THREE, T0).isEmpty());
    }

    @Test
    void an_empty_required_set_completes_via_complete() {
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        assertTrue(store.complete("alice@example.com", T0));
        assertFalse(store.complete("alice@example.com", T0), "already completed");
    }

    @Test
    void the_sweep_compensates_only_the_overdue_and_only_once() {
        store.start(UUID.randomUUID(), "old@example.com", T0);
        store.start(UUID.randomUUID(), "fresh@example.com", T0.plusSeconds(300));
        SagaStore.SweepResult swept =
                store.sweepOverdue(T0.plusSeconds(120), NO_RETRIES, T0.plusSeconds(400));
        assertEquals(List.of("old@example.com"),
                swept.compensated().stream().map(SagaStore.Compensated::email).toList());
        assertEquals(List.of(),
                store.sweepOverdue(T0.plusSeconds(120), NO_RETRIES, T0.plusSeconds(400)).compensated(),
                "a compensated saga does not compensate again");
    }

    /**
     * The retry timeline, with the clock that matters: every re-command buys the participant a
     * WHOLE purge timeout before the next decision. This test used to assert the opposite — three
     * retries inside three seconds of sweeps, because the deadline was measured from the saga's
     * birth and an overdue saga was a candidate on every single pass. That arithmetic put the
     * failure verdict on the wire ~45s after the deadline, while the participant still had a live
     * retry budget (90s) for the re-command it had just been sent: the purge then ran AFTER
     * security had unlocked the account and mailed "your deletion failed".
     */
    @Test
    void every_delivered_recommand_buys_a_whole_timeout_before_the_next_decision() {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.confirm("alice@example.com", null, "memes", THREE, T0);
        Instant lastAsked = T0;
        for (int attempt = 1; attempt <= 3; attempt++) {
            Instant tooEarly = lastAsked.plusSeconds(TIMEOUT - 1);
            assertEquals(List.of(), sweepAt(tooEarly).retries(),
                    "attempt " + attempt + " may not be due one second before the timeout since"
                            + " the last command — that is the participant's budget");
            Instant due = lastAsked.plusSeconds(TIMEOUT + 1);
            SagaStore.SweepResult swept = sweepAt(due);
            assertEquals(List.of(new SagaStore.Retry(saga, "alice@example.com")), swept.retries(),
                    "attempt " + attempt + " re-commands instead of giving up");
            assertEquals(List.of(), swept.compensated());
            assertTrue(store.retryDelivered(saga, due),   // the loop's word that the re-command
                    "a delivery against a STARTED saga must report the charge");   // reached Kafka
            lastAsked = due;
        }
        SagaStore.SweepResult early = sweepAt(lastAsked.plusSeconds(TIMEOUT - 1));
        assertEquals(List.of(), early.retries(), "the retries are spent");
        assertEquals(List.of(), early.compensated(),
                "and capitulating here would announce the failure while the participant is still"
                        + " working on the re-command it was just sent");
        SagaStore.SweepResult last = sweepAt(lastAsked.plusSeconds(TIMEOUT + 1));
        assertEquals(List.of(new SagaStore.Compensated(saga, "alice@example.com", Set.of("memes"))),
                last.compensated(),
                "capitulation names the participants that DID purge — the partial-purge disclosure");
        // 4 x 120s + 4s: the whole case outlives every one of the participant's 90s budgets, which
        // is the property the old 165s timeline broke
        assertEquals(T0.plusSeconds(4 * TIMEOUT + 4), lastAsked.plusSeconds(TIMEOUT + 1));
    }

    /** One sweep pass as the sweeper makes it: the cutoff is {@code now - purgeTimeout}. */
    private SagaStore.SweepResult sweepAt(Instant now) {
        return store.sweepOverdue(now.minusSeconds(TIMEOUT), 3, now);
    }

    @Test
    void an_undelivered_retry_burns_nothing_and_never_capitulates() {
        // the broker is down: every sweep offers the candidate, no retryDelivered() ever comes —
        // the counter must stay untouched and the saga must NOT compensate, or three sweeps
        // against a dead broker would announce a failure without one re-command on the wire
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        for (int sweep = 1; sweep <= 5; sweep++) {
            SagaStore.SweepResult swept =
                    store.sweepOverdue(T0.plusSeconds(120), 3, T0.plusSeconds(120L + sweep));
            assertEquals(List.of(new SagaStore.Retry(saga, "alice@example.com")), swept.retries(),
                    "sweep " + sweep + " still offers the SAME candidate — nothing was delivered");
            assertEquals(List.of(), swept.compensated(),
                    "no compensation may happen while no retry was ever delivered");
        }
    }

    @Test
    void the_stored_policy_rides_every_retry_candidate() {
        // the leaver's choices, stored at start (V3), must come back with the sweep's retry so
        // the re-commanded purge repeats the ORIGINAL command instead of the participants' defaults
        String policy = "{\"memes\":\"DELETE\",\"comments\":\"ANONYMIZE_AUTHOR\"}";
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", policy, T0);
        SagaStore.SweepResult swept = store.sweepOverdue(T0.plusSeconds(120), 3, T0.plusSeconds(130));
        assertEquals(List.of(new SagaStore.Retry(saga, "alice@example.com", policy)), swept.retries(),
                "the retry must carry the policy exactly as stored");
    }

    @Test
    void a_saga_started_without_policy_retries_without_one() {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        SagaStore.SweepResult swept = store.sweepOverdue(T0.plusSeconds(120), 3, T0.plusSeconds(130));
        assertEquals(List.of(new SagaStore.Retry(saga, "alice@example.com", null)), swept.retries(),
                "no stored choices means an honestly bare re-command — never an invented policy");
    }

    @Test
    void a_delivered_retry_stamps_the_callers_instant_on_the_saga() throws Exception {
        // updated_at is the sweep's clock now, so it has to come from the SAME clock the cutoff
        // does — the caller's. Taking the database's CURRENT_TIMESTAMP instead would put the skew
        // between two clocks straight into the participant's budget
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.retryDelivered(saga, T0.plusSeconds(130));
        assertEquals(T0.plusSeconds(130), updatedAtInDb(saga),
                "the delivered re-command is activity on the case, stamped when the caller says");
    }

    private Instant updatedAtInDb(UUID saga) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement select = connection.prepareStatement(
                     "SELECT updated_at FROM offboarding_sagas WHERE id = ?")) {
            select.setObject(1, saga);
            try (var rows = select.executeQuery()) {
                rows.next();
                return rows.getTimestamp(1).toInstant();
            }
        }
    }

    @Test
    void a_delivered_retry_is_not_counted_against_a_finished_saga() throws Exception {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.complete("alice@example.com", T0);
        // a late delivery report after completion must be a no-op — and must SAY so (false),
        // because the loop's retries-delivered metric counts only what was actually charged
        assertFalse(store.retryDelivered(saga, T0.plusSeconds(5)),
                "a no-op on a finished saga must not report a charge");
        assertEquals(0, retriesInDb(saga), "a finished saga's counter must stay untouched");
    }

    @Test
    void a_delivery_report_for_an_unknown_saga_reports_no_charge() {
        assertFalse(store.retryDelivered(UUID.randomUUID(), T0),
                "no saga, no charge — the metric must not count deliveries into the void");
    }

    private int retriesInDb(UUID saga) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement select = connection.prepareStatement(
                     "SELECT retries FROM offboarding_sagas WHERE id = ?")) {
            select.setObject(1, saga);
            try (var rows = select.executeQuery()) {
                rows.next();
                return rows.getInt(1);
            }
        }
    }

    @Test
    void a_completed_saga_never_compensates() {
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.confirm("alice@example.com", null, "memes", Set.of("memes"), T0);
        assertEquals(List.of(),
                store.sweepOverdue(T0.plusSeconds(9999), NO_RETRIES, T0.plusSeconds(10000)).compensated());
    }

    @Test
    void a_finished_saga_owes_its_outcome_until_marked_announced() {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.confirm("alice@example.com", null, "memes", Set.of("memes"), T0.plusSeconds(1));
        List<SagaStore.PendingOutcome> pending = store.unannouncedOutcomes(T0.plusSeconds(60));
        assertEquals(List.of(new SagaStore.PendingOutcome(saga, "alice@example.com", "COMPLETED", Set.of())),
                pending, "completing does NOT announce — the outbox owes the outcome");
        store.markAnnounced(saga);
        assertEquals(List.of(), store.unannouncedOutcomes(T0.plusSeconds(60)),
                "the announced mark settles the debt");
    }

    @Test
    void a_fresh_unannounced_outcome_is_not_republished_yet() {
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.confirm("alice@example.com", null, "memes", Set.of("memes"), T0.plusSeconds(50));
        assertEquals(List.of(), store.unannouncedOutcomes(T0.plusSeconds(50)),
                "the age guard keeps outcomes merely in flight from doubling");
    }

    @Test
    void a_compensated_outcome_carries_the_partial_purge() {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.confirm("alice@example.com", null, "comments", THREE, T0);
        store.sweepOverdue(T0.plusSeconds(120), NO_RETRIES, T0.plusSeconds(130));
        assertEquals(Set.of("comments"),
                store.unannouncedOutcomes(T0.plusSeconds(999)).get(0).confirmed());
    }

    @Test
    void the_retention_window_deletes_finished_and_announced_sagas_with_their_confirmations() {
        UUID old = store.start(UUID.randomUUID(), "old@example.com", T0);
        store.confirm("old@example.com", null, "memes", Set.of("memes"), T0.plusSeconds(1));
        store.markAnnounced(old);
        store.start(UUID.randomUUID(), "running@example.com", T0);
        UUID unannounced = store.start(UUID.randomUUID(), "owing@example.com", T0.plusSeconds(2));
        store.complete("owing@example.com", T0.plusSeconds(3));

        assertEquals(1, store.deleteFinishedBefore(T0.plusSeconds(60)),
                "only old + finished + announced goes; " + unannounced + " still owes its outcome");
        assertTrue(store.confirm("old@example.com", old, "memes", Set.of("memes"), T0.plusSeconds(61))
                        .isEmpty(),
                "the deleted saga is gone for confirmations too");
        assertEquals(List.of(), store.unannouncedOutcomes(T0.plusSeconds(1)).stream()
                        .filter(pending -> pending.email().equals("old@example.com")).toList(),
                "no orphaned rows left behind");
    }
}
