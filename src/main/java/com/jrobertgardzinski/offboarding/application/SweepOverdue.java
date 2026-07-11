package com.jrobertgardzinski.offboarding.application;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The timeout path: sagas whose participants never all confirmed within the deadline are
 * compensated, and the caller announces each failure so security can unlock the account and
 * apologise. Sweeping twice moves nothing twice — a saga compensates once.
 */
public class SweepOverdue {

    private final SagaStore sagas;
    private final Duration purgeTimeout;

    public SweepOverdue(SagaStore sagas, Duration purgeTimeout) {
        this.sagas = sagas;
        this.purgeTimeout = purgeTimeout;
    }

    public List<String> execute(Instant now) {
        return sagas.compensateOverdue(now.minus(purgeTimeout), now);
    }
}
