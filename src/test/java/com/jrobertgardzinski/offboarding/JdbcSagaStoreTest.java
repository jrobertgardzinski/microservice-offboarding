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
        assertTrue(store.confirm("alice@example.com", null, "memes", THREE, T0).isEmpty());
        assertTrue(store.confirm("alice@example.com", null, "memes", THREE, T0).isEmpty(),
                "duplicate is a no-op");
        assertTrue(store.confirm("alice@example.com", null, "comments", THREE, T0).isEmpty());
        assertTrue(store.confirm("alice@example.com", null, "collections", THREE, T0).isPresent(),
                "the last one completes");
        assertTrue(store.confirm("alice@example.com", null, "collections", THREE, T0).isEmpty(),
                "the once-latch: completion is reported to exactly one caller");
    }

    @Test
    void a_confirmation_addressed_by_saga_id_lands_on_that_saga() {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        assertEquals(saga,
                store.confirm("alice@example.com", saga, "memes", Set.of("memes"), T0).orElseThrow(),
                "the echoed saga id is the precise address");
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
        assertEquals(second,
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

    @Test
    void the_sweep_retries_before_capitulating() {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.confirm("alice@example.com", null, "memes", THREE, T0);
        for (int attempt = 1; attempt <= 3; attempt++) {
            SagaStore.SweepResult swept =
                    store.sweepOverdue(T0.plusSeconds(120), 3, T0.plusSeconds(120L + attempt));
            assertEquals(List.of(new SagaStore.Retry(saga, "alice@example.com")), swept.retries(),
                    "attempt " + attempt + " re-commands instead of giving up");
            assertEquals(List.of(), swept.compensated());
            assertTrue(store.retryDelivered(saga),   // the loop's word that the re-command
                    "a delivery against a STARTED saga must report the charge");   // reached the broker
        }
        SagaStore.SweepResult last = store.sweepOverdue(T0.plusSeconds(120), 3, T0.plusSeconds(200));
        assertEquals(List.of(), last.retries(), "the retries are spent");
        assertEquals(List.of(new SagaStore.Compensated(saga, "alice@example.com", Set.of("memes"))),
                last.compensated(),
                "capitulation names the participants that DID purge — the partial-purge disclosure");
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
    void a_delivered_retry_bumps_updated_at() throws Exception {
        UUID saga = store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.retryDelivered(saga);
        assertTrue(updatedAtInDb(saga).isAfter(T0),
                "the delivered re-command is activity on the case; updated_at must move");
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
        assertFalse(store.retryDelivered(saga),
                "a no-op on a finished saga must not report a charge");
        assertEquals(0, retriesInDb(saga), "a finished saga's counter must stay untouched");
    }

    @Test
    void a_delivery_report_for_an_unknown_saga_reports_no_charge() {
        assertFalse(store.retryDelivered(UUID.randomUUID()),
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
