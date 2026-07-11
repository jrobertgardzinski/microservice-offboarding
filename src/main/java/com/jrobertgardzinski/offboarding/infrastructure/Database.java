package com.jrobertgardzinski.offboarding.infrastructure;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

/**
 * Builds the {@link DataSource} and runs the Flyway migrations. With {@code DB_URL} set it is
 * Postgres; without it, an in-memory H2 in PostgreSQL mode — so dev and tests run the SAME JDBC
 * adapter and the SAME migrations the production database runs, with no second schema to drift.
 */
public final class Database {

    private Database() {
    }

    public static DataSource migratedDataSource() {
        String url = System.getenv().getOrDefault("DB_URL", "").trim();
        HikariConfig config = new HikariConfig();
        if (url.isEmpty()) {
            config.setJdbcUrl("jdbc:h2:mem:offboarding;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
            config.setUsername("sa");
            config.setPassword("");
        } else {
            config.setJdbcUrl(url);
            config.setUsername(System.getenv().getOrDefault("DB_USER", "postgres"));
            config.setPassword(System.getenv().getOrDefault("DB_PASSWORD", "secret"));
        }
        config.setMaximumPoolSize(10);
        DataSource dataSource = new HikariDataSource(config);
        Flyway.configure().dataSource(dataSource).load().migrate();
        return dataSource;
    }
}
