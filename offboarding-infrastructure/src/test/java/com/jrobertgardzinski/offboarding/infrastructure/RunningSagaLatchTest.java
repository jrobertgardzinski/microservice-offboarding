package com.jrobertgardzinski.offboarding.infrastructure;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The schema holds the per-address latch: one running saga per address, settled ones step aside. */
@Epic("Infrastructure")
@Feature("Schema")
class RunningSagaLatchTest {

    private static final Instant T0 = Instant.parse("2026-07-11T12:00:00Z");

    private final String url = "jdbc:h2:mem:latch-" + UUID.randomUUID()
            + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";

    @Test
    void a_second_running_saga_for_the_same_address_is_refused_by_the_schema() throws Exception {
        Flyway.configure().dataSource(url, "sa", "").locations("classpath:db/migration").load().migrate();

        insert("alice@example.com", "STARTED", "alice@example.com");
        insert("alice@example.com", "COMPENSATED", null);   // settled: not on the latch
        SQLException rejected = assertThrows(SQLException.class,
                () -> insert("alice@example.com", "STARTED", "alice@example.com"));
        assertEquals("23505", rejected.getSQLState(), "UNIQUE(running_email) is the latch");
    }

    private void insert(String email, String state, String runningEmail) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, "sa", "");
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO offboarding_sagas (id, fact_id, email, running_email, state, created_at, updated_at) "
                             + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            statement.setObject(1, UUID.randomUUID());
            statement.setObject(2, UUID.randomUUID());
            statement.setString(3, email);
            statement.setString(4, runningEmail);
            statement.setString(5, state);
            statement.setTimestamp(6, Timestamp.from(T0));
            statement.setTimestamp(7, Timestamp.from(T0));
            statement.executeUpdate();
        }
    }
}
