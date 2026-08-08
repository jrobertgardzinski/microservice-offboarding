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
import java.util.TreeSet;
import java.util.UUID;

/**
 * Saga progress in the database — the same adapter runs against Postgres (prod) and H2 in PG mode
 * (dev/tests). Confirmations are ROWS keyed (saga, participant), never per-participant columns:
 * a new participant is a configuration entry, not a migration. Idempotence leans on the primary
 * key (a duplicate confirmation is unique-violation → ignored) and the STARTED→COMPLETED update
 * is the once-latch — exactly one caller sees the saga complete.
 *
 * <p>Three things are captured when a saga OPENS and never read back from configuration:
 * the leaver's policy (V3), security's handle on the deletion (V5 — the verdict echoes it, so a
 * late verdict cannot be matched to the wrong request by email address) and the quorum of
 * participants this case must reach (V6 — re-configuring the participants must not redefine
 * completeness for cases already under way).
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
    public UUID start(Opening opening, Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            // two attempts at most: the read-then-insert can lose a race, but the UNIQUE
            // constraints turn the loss into a 23505 and the second read finds the winner's saga
            for (int attempt = 0; ; attempt++) {
                Optional<UUID> byFact = sagaOfFact(connection, opening.factId());
                if (byFact.isPresent()) {
                    return byFact.get();   // a replayed fact finds its saga, even a finished one
                }
                Optional<Target> running = runningSaga(connection, opening.email());
                if (running.isPresent()) {
                    // a second request joins the saga already underway — and hands it the handle
                    // of the security saga that is NOW waiting: security only asks again once its
                    // previous saga finished, so the older handle names a closed case and a
                    // verdict echoing it would land nowhere
                    adoptSecurityHandle(connection, running.get(), opening.securitySagaId());
                    return running.get().id();
                }
                UUID id = UUID.randomUUID();
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO offboarding_sagas "
                                + "(id, fact_id, email, running_email, state, policy, "
                                + "security_saga_id, required_participants, created_at, updated_at) "
                                + "VALUES (?, ?, ?, ?, 'STARTED', ?, ?, ?, ?, ?)")) {
                    insert.setObject(1, id);
                    insert.setObject(2, opening.factId());
                    insert.setString(3, opening.email());
                    insert.setString(4, opening.email());   // one-running-saga-per-email latch (V2)
                    insert.setString(5, opening.policy());  // the leaver's choices, verbatim (V3)
                    insert.setObject(6, opening.securitySagaId());   // security's handle (V5)
                    insert.setString(7, joined(opening.participants()));   // the quorum (V6)
                    insert.setTimestamp(8, Timestamp.from(at));
                    insert.setTimestamp(9, Timestamp.from(at));
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
    public Optional<Recorded> confirm(String email, UUID sagaId, String participant,
                                      Set<String> required, Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            // fresh confirmations echo the saga id the command carried — the precise address,
            // and the FINAL word: a sagaId whose saga is no longer STARTED is a stray from a
            // closed case, treated like a confirmation for an unknown saga. It must NOT fall
            // back to the email lookup — that would let an echo of a finished case land on a
            // NEWER saga for the same account. The email fallback exists solely for
            // confirmations without the sagaId field (old producers).
            Optional<Target> target;
            if (sagaId != null) {
                target = startedSaga(connection, sagaId);
            } else {
                target = runningSaga(connection, email);
            }
            if (target.isEmpty()) {
                return Optional.empty();   // a stray — no saga is waiting for this
            }
            UUID saga = target.get().id();
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
            // the quorum this saga OPENED with, never the current configuration: a shortened
            // OFFBOARDING_PARTICIPANTS would otherwise close a case whose missing participant was
            // commanded and never answered, and a lengthened one would make the case wait for a
            // participant that never got a command
            Set<String> quorum = target.get().recordedParticipants().orElse(required);
            boolean completed = confirmedParticipants(connection, saga).containsAll(quorum)
                    && completeStarted(connection, saga, at);
            // the policy rides back out with the completing confirmation: it is what the closure
            // command carries, and this is the moment the closure is sent
            return Optional.of(new Recorded(saga, target.get().securitySagaId(), completed,
                    target.get().policy()));
        } catch (SQLException e) {
            throw new IllegalStateException("could not record purge confirmation", e);
        }
    }

    @Override
    public boolean complete(String email, Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            Optional<Target> running = runningSaga(connection, email);
            return running.isPresent() && completeStarted(connection, running.get().id(), at);
        } catch (SQLException e) {
            throw new IllegalStateException("could not complete offboarding saga", e);
        }
    }

    @Override
    public SweepResult sweepOverdue(Instant cutoff, int maxRetries, Instant at) {
        List<Retry> retries = new ArrayList<>();
        List<Compensated> compensated = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            record Overdue(UUID id, String email, int retriesSoFar, String policy,
                           UUID securitySagaId) {
            }
            List<Overdue> overdue = new ArrayList<>();
            // updated_at, NOT created_at (V4 carries the index): the deadline is measured from the
            // last time the participant was ASKED, which is what retryDelivered() stamps. Measured
            // from birth, a saga past the deadline was a candidate again on every single pass, so
            // the whole retry budget burned down in three sweep intervals and the failure verdict
            // overtook the re-command the participant was still working on
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT id, email, retries, policy, security_saga_id FROM offboarding_sagas "
                            + "WHERE state = 'STARTED' AND updated_at < ?")) {
                select.setTimestamp(1, Timestamp.from(cutoff));
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        overdue.add(new Overdue(rows.getObject(1, UUID.class),
                                rows.getString(2), rows.getInt(3), rows.getString(4),
                                rows.getObject(5, UUID.class)));
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
                                    confirmedParticipants(connection, saga.id()),
                                    saga.securitySagaId()));
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
    public boolean retryDelivered(UUID sagaId, Instant at) {
        // the delivered-first discipline (see SagaStore): only a re-command the broker ACCEPTED
        // moves the counter, so the state guard keeps a late delivery off a finished saga.
        // updated_at moves with it, and it now MATTERS: the sweep's overdue clock runs from this
        // stamp, so it is the participant's budget for the command just sent. That is why the
        // stamp is the caller's instant and no longer the database's CURRENT_TIMESTAMP — the
        // cutoff comes from the application clock, and comparing it against a second clock would
        // charge the leaver's content for the skew between them. Only STARTED sagas qualify, so
        // the finished states' age guards never see it. The updated-row count IS the answer:
        // 0 rows means the no-op on a finished or unknown saga, and the caller's metric must not
        // count what was never charged
        try (Connection connection = dataSource.getConnection();
             PreparedStatement update = connection.prepareStatement(
                     "UPDATE offboarding_sagas SET retries = retries + 1, updated_at = ? "
                             + "WHERE id = ? AND state = 'STARTED'")) {
            update.setTimestamp(1, Timestamp.from(at));
            update.setObject(2, sagaId);
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
                    "SELECT id, email, state, security_saga_id, policy FROM offboarding_sagas "
                            + "WHERE state IN ('COMPLETED', 'COMPENSATED') "
                            + "AND outcome_announced = FALSE AND updated_at < ?")) {
                select.setTimestamp(1, Timestamp.from(olderThan));
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        pending.add(new PendingOutcome(rows.getObject(1, UUID.class),
                                rows.getString(2), rows.getString(3), Set.of(),
                                rows.getObject(4, UUID.class), rows.getString(5)));
                    }
                }
            }
            // a failed outcome names the participants that DID purge; fetch them second so the
            // main query stays a single index-friendly scan
            List<PendingOutcome> withConfirmations = new ArrayList<>(pending.size());
            for (PendingOutcome outcome : pending) {
                withConfirmations.add("COMPENSATED".equals(outcome.state())
                        ? new PendingOutcome(outcome.sagaId(), outcome.email(), outcome.state(),
                        confirmedParticipants(connection, outcome.sagaId()),
                        outcome.securitySagaId(), outcome.policy())
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

    /**
     * A saga a confirmation can land on, with the two things the landing needs: the quorum the
     * saga OPENED with (empty when the row predates the column — then the caller's configuration
     * decides), security's handle on the deletion, which the verdict echoes, and the leaver's
     * stored policy, which the CLOSURE command carries back to the participants when the last
     * confirmation lands (they apply their rule at erasure time, not at mark time).
     */
    private record Target(UUID id, Optional<Set<String>> recordedParticipants,
                          UUID securitySagaId, String policy) {
    }

    private static Optional<Target> runningSaga(Connection connection, String email) throws SQLException {
        // running_email is the V2 latch column: set while STARTED, NULL after — so this is both
        // the lookup and the uniqueness the constraint enforces
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id, required_participants, security_saga_id, policy FROM offboarding_sagas "
                        + "WHERE running_email = ?")) {
            select.setString(1, email);
            return target(select);
        }
    }

    private static Optional<Target> startedSaga(Connection connection, UUID sagaId) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id, required_participants, security_saga_id, policy FROM offboarding_sagas "
                        + "WHERE id = ? AND state = 'STARTED'")) {
            select.setObject(1, sagaId);
            return target(select);
        }
    }

    private static Optional<Target> target(PreparedStatement select) throws SQLException {
        try (ResultSet rows = select.executeQuery()) {
            return rows.next()
                    ? Optional.of(new Target(rows.getObject(1, UUID.class),
                    parsed(rows.getString(2)), rows.getObject(3, UUID.class), rows.getString(4)))
                    : Optional.empty();
        }
    }

    /**
     * Security's handle on a saga a NEWER fact just joined. Left alone when the newer fact carried
     * none, so a producer without the field can never erase a handle a previous one supplied.
     */
    private static void adoptSecurityHandle(Connection connection, Target running, UUID securitySagaId)
            throws SQLException {
        if (securitySagaId == null || securitySagaId.equals(running.securitySagaId())) {
            return;
        }
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE offboarding_sagas SET security_saga_id = ? WHERE id = ? AND state = 'STARTED'")) {
            update.setObject(1, securitySagaId);
            update.setObject(2, running.id());
            update.executeUpdate();
        }
    }

    /**
     * The recorded quorum, comma-separated (participant names cannot contain a comma — the
     * configuration spec itself is comma-separated, see Main.parseParticipants). NULL means the
     * saga recorded nothing, which is NOT the same as an empty quorum: the former defers to the
     * caller's configuration, the latter is a saga that waits for nobody.
     */
    private static Optional<Set<String>> parsed(String stored) {
        if (stored == null) {
            return Optional.empty();
        }
        return Optional.of(stored.isEmpty() ? Set.of() : Set.of(stored.split(",")));
    }

    private static String joined(Set<String> participants) {
        return participants == null ? null : String.join(",", new TreeSet<>(participants));
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
