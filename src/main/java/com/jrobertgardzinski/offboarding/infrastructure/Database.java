package com.jrobertgardzinski.offboarding.infrastructure;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * Builds the {@link DataSource} and runs the Flyway migrations. With {@code DB_URL} set it is
 * Postgres; without it, an in-memory H2 in PostgreSQL mode — so dev and tests run the SAME JDBC
 * adapter and the SAME migrations the production database runs, with no second schema to drift.
 *
 * <p><b>Every clock this DataSource can block on is EXPLICIT</b>, for exactly the reason the Kafka
 * clocks in {@link KafkaLoop} are: the saga loops call this store INSIDE the iteration whose gap
 * {@code /alive} measures, so an unbounded database wait is an unbounded liveness gap. pgjdbc
 * defaults {@code socketTimeout} to 0 — infinity — and a database that goes SILENT rather than
 * DOWN (a network partition, a frozen node, a DELETE queued behind somebody else's lock; a crashed
 * one is harmless, it answers RST at once) would then wedge the loop thread forever: no beat,
 * {@code /alive} 503, restart, and the new pod walks straight back into the same lock —
 * CrashLoopBackOff over a database problem a restart cannot fix. {@link #WORST_BLOCK} is the
 * bound, and {@link Main#ALIVE_STALL_FLOOR} is derived from it so the two cannot drift apart.
 */
public final class Database {

    /**
     * How long one statement may wait for bytes from a silent Postgres before the driver gives
     * up on the socket. pgjdbc's default is 0 = wait forever; this is the whole point of the
     * class-level note above.
     */
    static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(30);

    /**
     * How long {@code getConnection()} may wait for a connection from the pool (Hikari's own
     * clock, driver-independent — it covers both a pool with every connection busy and a new
     * physical connection that cannot be established). Hikari's default is 30s, which would add
     * a second unbounded-feeling block on top of the socket one.
     */
    static final Duration CONNECTION_TIMEOUT = Duration.ofSeconds(10);

    /**
     * The SERVER-side cap, sent as a session option in the connection's {@code options}
     * parameter. Deliberately BELOW {@link #SOCKET_TIMEOUT}: the two answer different halves of
     * the same failure. {@code socketTimeout} makes the CLIENT stop waiting, but a DELETE parked
     * behind another transaction's lock keeps burning a backend on the server until Postgres
     * notices the dead socket; {@code statement_timeout} makes the SERVER cancel it and hand back
     * a normal SQL error, which the loop's retry path already knows how to fail a pass on. Firing
     * first (25s < 30s) means the ordinary case is a clean error rather than a discarded
     * connection. Flyway shares this DataSource, so the migrations run under the same cap — they
     * are small DDL on small tables (see {@code db/migration}), so 25s is ample; a future
     * migration that genuinely needs longer must raise this or run outside the pool.
     */
    static final Duration STATEMENT_TIMEOUT = Duration.ofSeconds(25);

    /**
     * The longest ONE database interaction can legitimately hold a loop iteration, and therefore
     * the term {@link Main#ALIVE_STALL_FLOOR} carries for the store: the wait for a connection
     * plus one silent read on it. The two CHAIN in the worst case (a caller waits nearly the
     * whole connection timeout, is handed a connection, and that one then goes silent), so the
     * bound is their sum, not their maximum. Only one such block per iteration is counted: the
     * first database failure throws out of the pass, so the later store calls in the same
     * iteration never run.
     */
    static final Duration WORST_BLOCK = CONNECTION_TIMEOUT.plus(SOCKET_TIMEOUT);

    private Database() {
    }

    public static DataSource migratedDataSource() {
        DataSource dataSource = new HikariDataSource(
                config(System.getenv().getOrDefault("DB_URL", "").trim()));
        Flyway.configure().dataSource(dataSource).load().migrate();
        return dataSource;
    }

    /**
     * The pool's configuration, separated from the connecting so the test can pin the timeouts
     * {@link Main#ALIVE_STALL_FLOOR} is derived from without needing a database to connect to.
     * A blank url means the in-memory H2 (dev, tests): the Postgres-only driver properties are
     * skipped there — H2 refuses connection settings it does not know, and an in-memory database
     * has no socket to time out on anyway.
     */
    static HikariConfig config(String url) {
        HikariConfig config = new HikariConfig();
        if (url.isEmpty()) {
            config.setJdbcUrl("jdbc:h2:mem:offboarding;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
            config.setUsername("sa");
            config.setPassword("");
        } else {
            config.setJdbcUrl(url);
            config.setUsername(System.getenv().getOrDefault("DB_USER", "postgres"));
            config.setPassword(System.getenv().getOrDefault("DB_PASSWORD", "secret"));
            // driver properties, merged into the connection properties by Hikari's
            // DriverDataSource; a DB_URL that spells either of them out itself still wins, since
            // pgjdbc lets the URL's parameters override the ones passed in
            config.addDataSourceProperty("socketTimeout",
                    String.valueOf(SOCKET_TIMEOUT.toSeconds()));            // pgjdbc: seconds
            config.addDataSourceProperty("connectTimeout",
                    String.valueOf(CONNECTION_TIMEOUT.toSeconds()));        // the TCP handshake
            config.addDataSourceProperty("options",
                    "-c statement_timeout=" + STATEMENT_TIMEOUT.toMillis());
        }
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(CONNECTION_TIMEOUT.toMillis());
        return config;
    }
}
