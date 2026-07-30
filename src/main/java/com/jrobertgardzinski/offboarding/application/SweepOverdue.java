package com.jrobertgardzinski.offboarding.application;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * The sweeper's whole pass, three duties in one sweep. (1) The timeout path with a second wind:
 * an overdue saga is re-commanded up to {@code maxRetries} times — the participants are
 * idempotent, so retrying is free — and only then compensated, for the caller to announce the
 * failure. {@code purgeTimeout} is the silence allowed since the participant was last ASKED (the
 * store measures it from the last delivered command, see {@link SagaStore#sweepOverdue}), so the
 * whole case may take {@code purgeTimeout x (maxRetries + 1)} before the failure is announced —
 * long enough that every re-command's own retry budget at the participant has expired first.
 * A retry counts only once its re-command was DELIVERED (the caller reports it via
 * {@link SagaStore#retryDelivered}); an unreachable broker therefore burns no retries and can
 * never make the saga capitulate without a single command on the wire. (2) The outbox backlog: finished sagas whose outcome never got its announced mark are
 * handed back for re-publication — aged past {@code republishAfter} so outcomes merely in flight
 * are not doubled (the consumers' idempotence would absorb it anyway). (3) PII retention:
 * finished-and-announced sagas older than {@code retention} are deleted outright. Sweeping twice
 * moves nothing twice.
 */
public class SweepOverdue {

    /** The house defaults; production overrides ride the environment (see Main). */
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final Duration DEFAULT_REPUBLISH_AFTER = Duration.ofSeconds(30);
    public static final Duration DEFAULT_RETENTION = Duration.ofDays(30);

    /** Everything one pass decided: commands to resend, failures to announce, outcomes to redo. */
    public record Swept(List<SagaStore.Retry> retries,
                        List<SagaStore.Compensated> compensated,
                        List<SagaStore.PendingOutcome> unannounced) {
    }

    private final SagaStore sagas;
    private final Duration purgeTimeout;
    private final int maxRetries;
    private final Duration republishAfter;
    private final Duration retention;

    public SweepOverdue(SagaStore sagas, Duration purgeTimeout) {
        this(sagas, purgeTimeout, DEFAULT_MAX_RETRIES, DEFAULT_REPUBLISH_AFTER, DEFAULT_RETENTION);
    }

    public SweepOverdue(SagaStore sagas, Duration purgeTimeout, int maxRetries,
                        Duration republishAfter, Duration retention) {
        this.sagas = sagas;
        this.purgeTimeout = purgeTimeout;
        this.maxRetries = maxRetries;
        this.republishAfter = republishAfter;
        this.retention = retention;
    }

    public Swept execute(Instant now) {
        SagaStore.SweepResult overdue = sagas.sweepOverdue(now.minus(purgeTimeout), maxRetries, now);
        List<SagaStore.PendingOutcome> unannounced = sagas.unannouncedOutcomes(now.minus(republishAfter));
        sagas.deleteFinishedBefore(now.minus(retention));
        return new Swept(overdue.retries(), overdue.compensated(), unannounced);
    }
}
