package com.jrobertgardzinski.offboarding.infrastructure;

import com.jrobertgardzinski.offboarding.application.SagaStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Saga progress in the database — the same adapter runs against Postgres (prod) and H2 in PG mode
 * (dev/tests). Confirmations are ROWS keyed (saga, participant), never per-participant columns:
 * a new participant is a configuration entry, not a migration. Idempotence leans on the primary
 * key (a duplicate confirmation is unique-violation → ignored) and the STARTED→COMPLETED update
 * is the once-latch — exactly one caller sees the saga complete.
 *
 * <p>Two more constraints do the same job for starting: {@code fact_id UNIQUE} (V1) catches a
 * replayed fact that races its twin past the read-before-insert, and {@code running_email UNIQUE}
 * (V2 — set while STARTED, NULLed by every finishing update) catches two facts racing to open a
 * second saga for one account. Losing either race is a 23505, and the loser adopts the winner's
 * saga.
 */
public class JdbcSagaStore implements SagaStore {

    private static final String UNIQUE_VIOLATION = "23505";

    private final DataSource dataSource;

    public JdbcSagaStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public UUID start(UUID factId, String email, String policy, Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            // two attempts at most: the read-then-insert can lose a race, but the UNIQUE
            // constraints turn the loss into a 23505 and the second read finds the winner's saga
            for (int attempt = 0; ; attempt++) {
                Optional<UUID> byFact = sagaOfFact(connection, factId);
                if (byFact.isPresent()) {
                    return byFact.get();   // a replayed fact finds its saga, even a finished one
                }
                Optional<UUID> running = runningSaga(connection, email);
                if (running.isPresent()) {
                    return running.get();   // a second request joins the saga already underway
                }
                UUID id = UUID.randomUUID();
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO offboarding_sagas "
                                + "(id, fact_id, email, running_email, state, policy, created_at, updated_at) "
                                + "VALUES (?, ?, ?, ?, 'STARTED', ?, ?, ?)")) {
                    insert.setObject(1, id);
                    insert.setObject(2, factId);
                    insert.setString(3, email);
                    insert.setString(4, email);   // the one-running-saga-per-email latch (V2)
                    insert.setString(5, policy);  // the leaver's choices, verbatim (V3) — may be null
                    insert.setTimestamp(6, Timestamp.from(at));
                    insert.setTimestamp(7, Timestamp.from(at));
                    insert.executeUpdate();
                    return id;
                } catch (SQLException raced) {
                    if (!UNIQUE_VIOLATION.equals(raced.getSQLState()) || attempt > 0) {
                        throw raced;
                    }
                    // fact_id or running_email collided: someone inserted between our read and
                    // our insert — loop once more and adopt their saga instead of failing
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not start offboarding saga", e);
        }
    }

    private static Optional<UUID> sagaOfFact(Connection connection, UUID factId) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id FROM offboarding_sagas WHERE fact_id = ?")) {
            select.setObject(1, factId);
            try (ResultSet rows = select.executeQuery()) {
                return rows.next() ? Optional.of(rows.getObject(1, UUID.class)) : Optional.empty();
            }
        }
    }

    @Override
    public Optional<UUID> confirm(String email, UUID sagaId, String participant, Set<String> required,
                                  Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            // fresh confirmations echo the saga id the command carried — the precise address,
            // and the FINAL word: a sagaId whose saga is no longer STARTED is a stray from a
            // closed case, treated like a confirmation for an unknown saga. It must NOT fall
            // back to the email lookup — that would let an echo of a finished case land on a
            // NEWER saga for the same account. The email fallback exists solely for
            // confirmations without the sagaId field (old producers).
            Optional<UUID> target;
            if (sagaId != null) {
                target = startedSaga(connection, sagaId);
            } else {
                target = runningSaga(connection, email);
            }
            if (target.isEmpty()) {
                return Optional.empty();   // a stray — no saga is waiting for this
            }
            UUID saga = target.get();
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO offboarding_confirmations (saga_id, participant, confirmed_at) "
                            + "VALUES (?, ?, ?)")) {
                insert.setObject(1, saga);
                insert.setString(2, participant);
                insert.setTimestamp(3, Timestamp.from(at));
                insert.executeUpdate();
            } catch (SQLException duplicate) {
                if (!UNIQUE_VIOLATION.equals(duplicate.getSQLState())) {
                    throw duplicate;
                }
            }
            if (!confirmedParticipants(connection, saga).containsAll(required)) {
                return Optional.empty();
            }
            return completeStarted(connection, saga, at) ? Optional.of(saga) : Optional.empty();
        } catch (SQLException e) {
            throw new IllegalStateException("could not record purge confirmation", e);
        }
    }

    @Override
    public boolean complete(String email, Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            Optional<UUID> running = runningSaga(connection, email);
            return running.isPresent() && completeStarted(connection, running.get(), at);
        } catch (SQLException e) {
            throw new IllegalStateException("could not complete offboarding saga", e);
        }
    }

    @Override
    public SweepResult sweepOverdue(Instant cutoff, int maxRetries, Instant at) {
        List<Retry> retries = new ArrayList<>();
        List<Compensated> compensated = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            record Overdue(UUID id, String email, int retriesSoFar, String policy) {
            }
            List<Overdue> overdue = new ArrayList<>();
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT id, email, retries, policy FROM offboarding_sagas "
                            + "WHERE state = 'STARTED' AND created_at < ?")) {
                select.setTimestamp(1, Timestamp.from(cutoff));
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        overdue.add(new Overdue(rows.getObject(1, UUID.class),
                                rows.getString(2), rows.getInt(3), rows.getString(4)));
                    }
                }
            }
            for (Overdue saga : overdue) {
                if (saga.retriesSoFar() < maxRetries) {
                    // participants are idempotent, so re-commanding costs nothing — hand the
                    // candidate back WITHOUT counting: the attempt is charged by retryDelivered()
                    // only after the resent PURGE_USER_CONTENT demonstrably reached the broker.
                    // Counting here would let a dead broker burn all retries without a single
                    // command on the wire, and the saga would compensate having never re-asked.
                    // The stored policy rides along so the re-command repeats the original
                    retries.add(new Retry(saga.id(), saga.email(), saga.policy()));
                } else {
                    // retries exhausted — give up, freeing the email for a future saga, and tell
                    // the caller who DID confirm so the failure can name the partial purge
                    try (PreparedStatement update = connection.prepareStatement(
                            "UPDATE offboarding_sagas SET state = 'COMPENSATED', running_email = NULL, "
                                    + "updated_at = ? WHERE id = ? AND state = 'STARTED'")) {
                        update.setTimestamp(1, Timestamp.from(at));
                        update.setObject(2, saga.id());
                        if (update.executeUpdate() == 1) {
                            compensated.add(new Compensated(saga.id(), saga.email(),
                                    confirmedParticipants(connection, saga.id())));
                        }
                    }
                }
            }
            return new SweepResult(retries, compensated);
        } catch (SQLException e) {
            throw new IllegalStateException("could not compensate overdue sagas", e);
        }
    }

    @Override
    public boolean retryDelivered(UUID sagaId) {
        // the delivered-first discipline (see SagaStore): only a re-command the broker ACCEPTED
        // moves the counter, so the state guard keeps a late delivery off a finished saga.
        // updated_at moves too — the database's own clock, because this method is the transport's
        // report of a delivery that JUST happened; the saga's business timestamps stay the
        // caller's. Only STARTED sagas qualify, so the finished states' age guards never see it.
        // The updated-row count IS the answer: 0 rows means the no-op on a finished or unknown
        // saga, and the caller's metric must not count what was never charged
        try (Connection connection = dataSource.getConnection();
             PreparedStatement update = connection.prepareStatement(
                     "UPDATE offboarding_sagas SET retries = retries + 1, "
                             + "updated_at = CURRENT_TIMESTAMP "
                             + "WHERE id = ? AND state = 'STARTED'")) {
            update.setObject(1, sagaId);
            return update.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException("could not count the delivered retry", e);
        }
    }

    @Override
    public void markAnnounced(UUID sagaId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement update = connection.prepareStatement(
                     "UPDATE offboarding_sagas SET outcome_announced = TRUE WHERE id = ?")) {
            update.setObject(1, sagaId);
            update.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("could not mark the outcome announced", e);
        }
    }

    @Override
    public List<PendingOutcome> unannouncedOutcomes(Instant olderThan) {
        List<PendingOutcome> pending = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT id, email, state FROM offboarding_sagas "
                            + "WHERE state IN ('COMPLETED', 'COMPENSATED') "
                            + "AND outcome_announced = FALSE AND updated_at < ?")) {
                select.setTimestamp(1, Timestamp.from(olderThan));
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        pending.add(new PendingOutcome(rows.getObject(1, UUID.class),
                                rows.getString(2), rows.getString(3), Set.of()));
                    }
                }
            }
            // a failed outcome names the participants that DID purge; fetch them second so the
            // main query stays a single index-friendly scan
            List<PendingOutcome> withConfirmations = new ArrayList<>(pending.size());
            for (PendingOutcome outcome : pending) {
                withConfirmations.add("COMPENSATED".equals(outcome.state())
                        ? new PendingOutcome(outcome.sagaId(), outcome.email(), outcome.state(),
                        confirmedParticipants(connection, outcome.sagaId()))
                        : outcome);
            }
            return withConfirmations;
        } catch (SQLException e) {
            throw new IllegalStateException("could not read the unannounced outcomes", e);
        }
    }

    @Override
    public int deleteFinishedBefore(Instant olderThan) {
        // two autocommit statements, children first: if the second fails the sagas simply survive
        // one more pass and the next sweep finishes the job
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement confirmations = connection.prepareStatement(
                    "DELETE FROM offboarding_confirmations WHERE saga_id IN "
                            + "(SELECT id FROM offboarding_sagas WHERE state IN ('COMPLETED', 'COMPENSATED') "
                            + "AND outcome_announced = TRUE AND updated_at < ?)")) {
                confirmations.setTimestamp(1, Timestamp.from(olderThan));
                confirmations.executeUpdate();
            }
            try (PreparedStatement sagas = connection.prepareStatement(
                    "DELETE FROM offboarding_sagas WHERE state IN ('COMPLETED', 'COMPENSATED') "
                            + "AND outcome_announced = TRUE AND updated_at < ?")) {
                sagas.setTimestamp(1, Timestamp.from(olderThan));
                return sagas.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("could not apply the retention window", e);
        }
    }

    private static Optional<UUID> runningSaga(Connection connection, String email) throws SQLException {
        // running_email is the V2 latch column: set while STARTED, NULL after — so this is both
        // the lookup and the uniqueness the constraint enforces
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id FROM offboarding_sagas WHERE running_email = ?")) {
            select.setString(1, email);
            try (ResultSet rows = select.executeQuery()) {
                return rows.next() ? Optional.of(rows.getObject(1, UUID.class)) : Optional.empty();
            }
        }
    }

    private static Optional<UUID> startedSaga(Connection connection, UUID sagaId) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id FROM offboarding_sagas WHERE id = ? AND state = 'STARTED'")) {
            select.setObject(1, sagaId);
            try (ResultSet rows = select.executeQuery()) {
                return rows.next() ? Optional.of(rows.getObject(1, UUID.class)) : Optional.empty();
            }
        }
    }

    private static Set<String> confirmedParticipants(Connection connection, UUID sagaId) throws SQLException {
        Set<String> confirmed = new HashSet<>();
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT participant FROM offboarding_confirmations WHERE saga_id = ?")) {
            select.setObject(1, sagaId);
            try (ResultSet rows = select.executeQuery()) {
                while (rows.next()) {
                    confirmed.add(rows.getString(1));
                }
            }
        }
        return confirmed;
    }

    /**
     * The once-latch: only the update that actually flips STARTED reports completion. Clearing
     * running_email in the same statement releases the per-email latch atomically, and the
     * outcome_announced flag deliberately stays FALSE — announcing is the outbox's job.
     */
    private static boolean completeStarted(Connection connection, UUID sagaId, Instant at) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE offboarding_sagas SET state = 'COMPLETED', running_email = NULL, updated_at = ? "
                        + "WHERE id = ? AND state = 'STARTED'")) {
            update.setTimestamp(1, Timestamp.from(at));
            update.setObject(2, sagaId);
            return update.executeUpdate() == 1;
        }
    }
}
