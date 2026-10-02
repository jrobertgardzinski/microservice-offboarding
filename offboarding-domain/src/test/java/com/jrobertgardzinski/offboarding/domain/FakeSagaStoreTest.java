package com.jrobertgardzinski.offboarding.domain;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;

/**
 * The in-memory double, held to {@link SagaStoreContractTest}.
 *
 * <p>It had no test of its own until 29.09.2026 — only the comments in it, each one claiming to
 * mirror the JDBC adapter, and two use-case tests that happened to run over it. Meanwhile
 * {@code portal-specs} built the whole portal on this class and its races layer learnt to snapshot
 * it, roll it back and read its outbox query. A stand-in that drifts does not fail; it makes a green
 * suite say something about a service that does not exist.
 */
@Epic("Saga")
@Feature("Saga store contract")
class FakeSagaStoreTest extends SagaStoreContractTest {

    private final FakeSagaStore store = new FakeSagaStore();

    @Override
    protected SagaStore store() {
        return store;
    }
}
