package com.jrobertgardzinski.offboarding.system;

import com.jrobertgardzinski.offboarding.domain.SagaStore;
import com.jrobertgardzinski.offboarding.domain.Compensated;
import com.jrobertgardzinski.offboarding.domain.PendingOutcome;
import com.jrobertgardzinski.offboarding.domain.Retry;
import com.jrobertgardzinski.offboarding.domain.SweepResult;
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
    /**
     * How long a participant may stay silent after it was ASKED — measured from the last command
     * it actually received, never from the saga's birth.
     *
     * <p>This number and {@link #DEFAULT_MAX_RETRIES} are HALF OF A CONTRACT WITH SECURITY, which
     * is why they live together and why {@link #worstCaseDecision()} exists: the whole case takes
     * at most timeout x (retries + 1), and security's own safety net must stay ABOVE that, or the
     * account comes back before its content is gone. That is not hypothetical — measured on the
     * live stack on 2026-08-08, security gave up at ~5 minutes while the portal was still working
     * and finished at ~8. Nobody decided that; it was the sum of two timeouts nobody read together.
     *
     * <p>Neither is settable from the environment, deliberately. A value that may not be tuned
     * without re-deriving somebody else's is not configuration: exposing it as a dial invites
     * exactly the drift above, and a knob nothing sets is machinery guarding a door nobody opens.
     * Changing it means a new image, which this service is built to survive — stateless, saga
     * state in Postgres, idempotent, offsets at the broker, so a redeploy delays deletions rather
     * than losing them.
     */
    public static final Duration DEFAULT_PURGE_TIMEOUT = Duration.ofSeconds(120);

    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final Duration DEFAULT_REPUBLISH_AFTER = Duration.ofSeconds(30);
    public static final Duration DEFAULT_RETENTION = Duration.ofDays(30);

    /** Everything one pass decided: commands to resend, failures to announce, outcomes to redo. */
    public record Swept(List<Retry> retries,
                        List<Compensated> compensated,
                        List<PendingOutcome> unannounced) {
    }

    private final SagaStore sagas;
    private final Duration purgeTimeout;
    private final int maxRetries;
    private final Duration republishAfter;
    private final Duration retention;

    /**
     * The longest a case can take before the failure is announced: every command gets the full
     * timeout, and there are retries + 1 of them. The number security must outlast.
     */
    public static Duration worstCaseDecision() {
        return DEFAULT_PURGE_TIMEOUT.multipliedBy(DEFAULT_MAX_RETRIES + 1L);
    }

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
        SweepResult overdue = sagas.sweepOverdue(now.minus(purgeTimeout), maxRetries, now);
        List<PendingOutcome> unannounced = sagas.unannouncedOutcomes(now.minus(republishAfter));
        sagas.deleteFinishedBefore(now.minus(retention));
        return new Swept(overdue.retries(), overdue.compensated(), unannounced);
    }
}
