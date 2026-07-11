package com.jrobertgardzinski.offboarding;

import com.jrobertgardzinski.offboarding.infrastructure.Database;
import com.jrobertgardzinski.offboarding.infrastructure.JdbcSagaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The JDBC adapter on the same H2 (PostgreSQL mode) the dev profile runs — the same Flyway
 * migrations as production, no second schema to drift. Exercises what the in-memory double
 * mirrors: the running-saga lookup, the once-latch on completion, and the overdue sweep.
 */
class JdbcSagaStoreTest {

    private static final Set<String> THREE = Set.of("memes", "comments", "collections");
    private static final Instant T0 = Instant.parse("2026-07-11T12:00:00Z");

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
    void only_the_last_required_confirmation_completes_and_only_once() {
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        assertFalse(store.confirm("alice@example.com", "memes", THREE, T0));
        assertFalse(store.confirm("alice@example.com", "memes", THREE, T0), "duplicate is a no-op");
        assertFalse(store.confirm("alice@example.com", "comments", THREE, T0));
        assertTrue(store.confirm("alice@example.com", "collections", THREE, T0), "the last one completes");
        assertFalse(store.confirm("alice@example.com", "collections", THREE, T0),
                "the once-latch: completion is reported to exactly one caller");
    }

    @Test
    void a_stray_confirmation_records_nothing() {
        assertFalse(store.confirm("nobody@example.com", "memes", THREE, T0));
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
        List<String> compensated = store.compensateOverdue(T0.plusSeconds(120), T0.plusSeconds(400));
        assertEquals(List.of("old@example.com"), compensated);
        assertEquals(List.of(), store.compensateOverdue(T0.plusSeconds(120), T0.plusSeconds(400)),
                "a compensated saga does not compensate again");
    }

    @Test
    void a_completed_saga_never_compensates() {
        store.start(UUID.randomUUID(), "alice@example.com", T0);
        store.confirm("alice@example.com", "memes", Set.of("memes"), T0);
        assertEquals(List.of(), store.compensateOverdue(T0.plusSeconds(9999), T0.plusSeconds(10000)));
    }
}
