package com.jrobertgardzinski.offboarding.infrastructure;

import com.jrobertgardzinski.offboarding.system.SagaStore;
import com.jrobertgardzinski.offboarding.system.SagaStoreContractTest;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;

import javax.sql.DataSource;

/**
 * The adapter the service runs on, held to the same {@link SagaStoreContractTest} as the in-memory
 * double — the whole point of the contract being a class and not a comment.
 *
 * <p>No clean slate: every case here is opened under an address of its own, freshly minted per test
 * method, which is what lets this share the migrated database with {@code JdbcSagaStoreTest} and
 * with whatever else runs against it. That test keeps what only the adapter can answer: the V2
 * UNIQUE constraints under real thread races, and the once-latch as one conditional UPDATE.
 */
@Epic("Infrastructure")
@Feature("Saga store contract")
class JdbcSagaStoreContractTest extends SagaStoreContractTest {

    private final DataSource dataSource = Database.migratedDataSource();

    private final JdbcSagaStore store = new JdbcSagaStore(dataSource);

    @Override
    protected SagaStore store() {
        return store;
    }
}
