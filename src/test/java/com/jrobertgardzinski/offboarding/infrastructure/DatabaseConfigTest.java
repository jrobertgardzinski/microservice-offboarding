package com.jrobertgardzinski.offboarding.infrastructure;

import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The database clocks must actually REACH the DataSource, not merely exist as constants. The
 * finding behind this: the pool was built without a socket timeout, and pgjdbc's default is 0 —
 * wait forever. A database that goes silent rather than down (a network partition, a frozen node,
 * a DELETE queued behind another transaction's lock) then wedges the saga loop inside the very
 * iteration {@code /alive} measures: no beat, 503, restart, and the new pod walks into the same
 * lock. So this pins the properties on the config Hikari is handed — a constant nobody passes on
 * is worth nothing, and the floor in {@link Main} is derived from these same values.
 */
class DatabaseConfigTest {

    private static final String POSTGRES = "jdbc:postgresql://offboarding-postgres:5432/offboarding";

    @Test
    void the_socket_timeout_reaches_the_postgres_driver_properties() {
        // pgjdbc reads socketTimeout in SECONDS; passing milliseconds here would quietly mean
        // eight hours, which is indistinguishable from the unbounded default this exists to fix
        assertEquals(String.valueOf(Database.SOCKET_TIMEOUT.toSeconds()),
                Database.config(POSTGRES).getDataSourceProperties().getProperty("socketTimeout"),
                "socketTimeout must be handed to the driver, in seconds");
    }

    @Test
    void the_connection_timeouts_reach_hikari_and_the_driver() {
        HikariConfig config = Database.config(POSTGRES);
        assertEquals(Database.CONNECTION_TIMEOUT.toMillis(), config.getConnectionTimeout(),
                "Hikari's own wait for a connection must be bounded too — its default is 30s");
        assertEquals(String.valueOf(Database.CONNECTION_TIMEOUT.toSeconds()),
                config.getDataSourceProperties().getProperty("connectTimeout"),
                "and the TCP handshake underneath it, in the driver's seconds");
    }

    @Test
    void the_statement_timeout_rides_along_as_a_session_option() {
        // the server-side half: socketTimeout stops the CLIENT waiting, but the DELETE parked
        // behind somebody else's lock keeps a backend busy until Postgres notices the dead
        // socket. statement_timeout makes the server cancel it and answer with a plain SQL error
        assertEquals("-c statement_timeout=" + Database.STATEMENT_TIMEOUT.toMillis(),
                Database.config(POSTGRES).getDataSourceProperties().getProperty("options"),
                "statement_timeout must be sent as a session option, in milliseconds");
        assertTrue(Database.STATEMENT_TIMEOUT.compareTo(Database.SOCKET_TIMEOUT) < 0,
                "and it must fire FIRST, so the ordinary case is a clean error rather than a"
                        + " connection thrown away mid-query");
    }

    @Test
    void the_in_memory_h2_gets_hikaris_clock_but_none_of_the_postgres_properties() {
        // H2 refuses connection settings it does not know, and an in-memory database has no
        // socket to time out on: dev and every test in this suite boot through this branch
        HikariConfig config = Database.config("");
        assertTrue(config.getJdbcUrl().startsWith("jdbc:h2:mem:"),
                "a blank DB_URL still means the in-memory H2: " + config.getJdbcUrl());
        assertNull(config.getDataSourceProperties().getProperty("socketTimeout"));
        assertNull(config.getDataSourceProperties().getProperty("options"));
        assertEquals(Database.CONNECTION_TIMEOUT.toMillis(), config.getConnectionTimeout(),
                "Hikari's own timeout is driver-independent and applies to H2 as well");
    }
}
