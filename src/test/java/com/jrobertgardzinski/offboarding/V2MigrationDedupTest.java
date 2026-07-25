package com.jrobertgardzinski.offboarding;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V2 against the dirty data V2 exists to outlaw: the old read-then-insert race could fork
 * several STARTED sagas for one email, and {@code ADD CONSTRAINT UNIQUE(running_email)} over
 * that dirt would fail the whole migration. The dedup in V2 must keep the NEWEST fork running,
 * close the older ones as COMPENSATED with {@code running_email} NULL — and deliberately leave
 * their {@code outcome_announced} FALSE, so the sweeper announces PORTAL_PURGE_FAILED for the
 * abandoned case (the correct outcome), while genuinely finished pre-V2 sagas ARE grandfathered
 * as announced.
 *
 * <p>Flyway is driven by hand here — {@code target("1")}, raw duplicate INSERTs no live code
 * can produce any more, then {@code target("2")} — on a fresh H2 (PG mode) per test, exactly
 * the engine the dev profile migrates.
 */
class V2MigrationDedupTest {

    private static final Instant T0 = Instant.parse("2026-07-11T12:00:00Z");

    private final String url = "jdbc:h2:mem:migration-" + UUID.randomUUID()
            + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";

    private void migrateTo(String version) {
        Flyway.configure()
                .dataSource(url, "sa", "")
                .locations("classpath:db/migration")
                .target(version)
                .load()
                .migrate();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(url, "sa", "");
    }

    /** A V1-shaped row — no running_email/outcome_announced/retries columns exist yet. */
    private void seedV1Saga(UUID id, String email, String state, Instant createdAt) throws SQLException {
        try (Connection connection = connect();
             PreparedStatement insert = connection.prepareStatement(
                     "INSERT INTO offboarding_sagas (id, fact_id, email, state, created_at, updated_at) "
                             + "VALUES (?, ?, ?, ?, ?, ?)")) {
            insert.setObject(1, id);
            insert.setObject(2, UUID.randomUUID());
            insert.setString(3, email);
            insert.setString(4, state);
            insert.setTimestamp(5, Timestamp.from(createdAt));
            insert.setTimestamp(6, Timestamp.from(createdAt));
            insert.executeUpdate();
        }
    }

    private record SagaRow(String state, String runningEmail, boolean announced) {
    }

    private SagaRow row(UUID id) throws SQLException {
        try (Connection connection = connect();
             PreparedStatement select = connection.prepareStatement(
                     "SELECT state, running_email, outcome_announced FROM offboarding_sagas WHERE id = ?")) {
            select.setObject(1, id);
            try (ResultSet rows = select.executeQuery()) {
                assertTrue(rows.next(), "saga " + id + " must survive the migration");
                return new SagaRow(rows.getString(1), rows.getString(2), rows.getBoolean(3));
            }
        }
    }

    @Test
    void v2_survives_forked_started_sagas_and_keeps_only_the_newest_running() throws Exception {
        migrateTo("1");
        // the dirt: two STARTED sagas for alice (the old race), plus innocent bystanders
        UUID aliceOlder = UUID.randomUUID();
        UUID aliceNewer = UUID.randomUUID();
        UUID bobRunning = UUID.randomUUID();
        UUID carolFinished = UUID.randomUUID();
        seedV1Saga(aliceOlder, "alice@example.com", "STARTED", T0);
        seedV1Saga(aliceNewer, "alice@example.com", "STARTED", T0.plusSeconds(60));
        seedV1Saga(bobRunning, "bob@example.com", "STARTED", T0);
        seedV1Saga(carolFinished, "carol@example.com", "COMPLETED", T0);

        migrateTo("2");   // must NOT fail on the duplicate — that is the whole point

        SagaRow newest = row(aliceNewer);
        assertEquals("STARTED", newest.state(), "the newest fork keeps running");
        assertEquals("alice@example.com", newest.runningEmail(), "and holds the per-email latch");

        SagaRow abandoned = row(aliceOlder);
        assertEquals("COMPENSATED", abandoned.state(), "the older fork is closed, not left to trip the constraint");
        assertEquals(null, abandoned.runningEmail(), "a closed fork must not occupy the latch");
        assertFalse(abandoned.announced(),
                "the abandoned fork was never announced — the sweeper must still announce "
                        + "PORTAL_PURGE_FAILED for it, so the grandfather clause may not touch it");

        SagaRow bob = row(bobRunning);
        assertEquals("STARTED", bob.state());
        assertEquals("bob@example.com", bob.runningEmail(), "a lone running saga is untouched");

        SagaRow carol = row(carolFinished);
        assertEquals("COMPLETED", carol.state());
        assertEquals(null, carol.runningEmail());
        assertTrue(carol.announced(),
                "sagas finished before V2 lived under fire-and-forget: grandfathered as announced");
    }

    @Test
    void v2_ties_on_created_at_break_deterministically_by_id() throws Exception {
        migrateTo("1");
        UUID low = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID high = UUID.fromString("00000000-0000-0000-0000-000000000002");
        seedV1Saga(low, "tie@example.com", "STARTED", T0);
        seedV1Saga(high, "tie@example.com", "STARTED", T0);

        migrateTo("2");

        assertEquals("STARTED", row(high).state(), "same created_at: the greater id wins");
        assertEquals("COMPENSATED", row(low).state());
    }

    @Test
    void the_constraint_added_over_deduplicated_data_still_bites() throws Exception {
        migrateTo("1");
        seedV1Saga(UUID.randomUUID(), "alice@example.com", "STARTED", T0);
        seedV1Saga(UUID.randomUUID(), "alice@example.com", "STARTED", T0.plusSeconds(60));
        migrateTo("2");

        try (Connection connection = connect();
             PreparedStatement insert = connection.prepareStatement(
                     "INSERT INTO offboarding_sagas "
                             + "(id, fact_id, email, running_email, state, created_at, updated_at) "
                             + "VALUES (?, ?, 'alice@example.com', 'alice@example.com', 'STARTED', ?, ?)")) {
            insert.setObject(1, UUID.randomUUID());
            insert.setObject(2, UUID.randomUUID());
            insert.setTimestamp(3, Timestamp.from(T0));
            insert.setTimestamp(4, Timestamp.from(T0));
            SQLException rejected = assertThrows(SQLException.class, insert::executeUpdate);
            assertEquals("23505", rejected.getSQLState(),
                    "after the dedup the UNIQUE(running_email) must be in force");
        }
    }
}
