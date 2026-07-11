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
 */
public class JdbcSagaStore implements SagaStore {

    private static final String UNIQUE_VIOLATION = "23505";

    private final DataSource dataSource;

    public JdbcSagaStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public UUID start(UUID factId, String email, Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            Optional<UUID> byFact = sagaOfFact(connection, factId);
            if (byFact.isPresent()) {
                return byFact.get();   // a replayed fact finds its saga, even a finished one
            }
            Optional<UUID> running = runningSaga(connection, email);
            if (running.isPresent()) {
                return running.get();
            }
            UUID id = UUID.randomUUID();
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO offboarding_sagas (id, fact_id, email, state, created_at, updated_at) "
                            + "VALUES (?, ?, ?, 'STARTED', ?, ?)")) {
                insert.setObject(1, id);
                insert.setObject(2, factId);
                insert.setString(3, email);
                insert.setTimestamp(4, Timestamp.from(at));
                insert.setTimestamp(5, Timestamp.from(at));
                insert.executeUpdate();
            }
            return id;
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
    public boolean confirm(String email, String participant, Set<String> required, Instant at) {
        try (Connection connection = dataSource.getConnection()) {
            Optional<UUID> running = runningSaga(connection, email);
            if (running.isEmpty()) {
                return false;   // a stray — no saga is waiting for this
            }
            UUID sagaId = running.get();
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO offboarding_confirmations (saga_id, participant, confirmed_at) "
                            + "VALUES (?, ?, ?)")) {
                insert.setObject(1, sagaId);
                insert.setString(2, participant);
                insert.setTimestamp(3, Timestamp.from(at));
                insert.executeUpdate();
            } catch (SQLException duplicate) {
                if (!UNIQUE_VIOLATION.equals(duplicate.getSQLState())) {
                    throw duplicate;
                }
            }
            if (!confirmedParticipants(connection, sagaId).containsAll(required)) {
                return false;
            }
            return completeStarted(connection, sagaId, at);
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
    public List<String> compensateOverdue(Instant cutoff, Instant at) {
        List<String> emails = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            List<UUID> overdue = new ArrayList<>();
            List<String> overdueEmails = new ArrayList<>();
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT id, email FROM offboarding_sagas WHERE state = 'STARTED' AND created_at < ?")) {
                select.setTimestamp(1, Timestamp.from(cutoff));
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        overdue.add(rows.getObject(1, UUID.class));
                        overdueEmails.add(rows.getString(2));
                    }
                }
            }
            for (int i = 0; i < overdue.size(); i++) {
                try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE offboarding_sagas SET state = 'COMPENSATED', updated_at = ? "
                                + "WHERE id = ? AND state = 'STARTED'")) {
                    update.setTimestamp(1, Timestamp.from(at));
                    update.setObject(2, overdue.get(i));
                    if (update.executeUpdate() == 1) {
                        emails.add(overdueEmails.get(i));
                    }
                }
            }
            return emails;
        } catch (SQLException e) {
            throw new IllegalStateException("could not compensate overdue sagas", e);
        }
    }

    private static Optional<UUID> runningSaga(Connection connection, String email) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id FROM offboarding_sagas WHERE email = ? AND state = 'STARTED' "
                        + "ORDER BY created_at DESC LIMIT 1")) {
            select.setString(1, email);
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

    /** The once-latch: only the update that actually flips STARTED reports completion. */
    private static boolean completeStarted(Connection connection, UUID sagaId, Instant at) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE offboarding_sagas SET state = 'COMPLETED', updated_at = ? "
                        + "WHERE id = ? AND state = 'STARTED'")) {
            update.setTimestamp(1, Timestamp.from(at));
            update.setObject(2, sagaId);
            return update.executeUpdate() == 1;
        }
    }
}
