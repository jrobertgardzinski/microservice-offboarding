package com.jrobertgardzinski.offboarding.application;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Persistence of the portal-side account-deletion saga: STARTED when security announces the
 * deletion fact; one confirmation per content participant; COMPLETED the moment the LAST required
 * participant confirmed; COMPENSATED when confirmations never came despite the sweeper's retries.
 * The required set is the caller's CONFIGURATION — the store records confirmations by name and
 * never hardcodes who participates. Transitions are idempotent — at-least-once delivery makes
 * duplicates a fact of life — and the STARTED→COMPLETED update is the once-latch: exactly one
 * call learns it completed.
 *
 * <p>Finishing a saga does NOT mean its outcome reached the broker: the mini-outbox flag
 * ({@code outcome_announced}) is set separately, by the caller, only after a successful publish
 * — and {@link #unannouncedOutcomes} is how the sweeper finds what still owes the world an
 * announcement.
 */
public interface SagaStore {

    /**
     * Everything a saga is opened WITH — the fields that have to be captured when the case opens
     * instead of being read back from configuration later.
     *
     * <p>{@code securitySagaId} is the handle security stamped on the deletion fact. It is stored
     * so the portal's verdict can ECHO it: without it security has to match verdicts by email
     * address, and a late verdict of a closed case then settles a NEWER deletion for the same
     * address. Null for a fact that carried no handle (an older producer).
     *
     * <p>{@code participants} is the required set AS CONFIGURED WHEN THIS SAGA OPENED. Null means
     * "not recorded" — a row from before the column existed, or a caller with no set to record —
     * and then completeness falls back to the caller's configuration, exactly as it used to.
     */
    record Opening(UUID factId, String email, String policy, UUID securitySagaId,
                   Set<String> participants) {
    }

    /**
     * Where one confirmation landed. {@code completedSaga} is the once-latch: true only for the
     * call that turned the LAST required confirmation into COMPLETED. A confirmation that landed
     * on no saga at all is {@link Optional#empty()} — a stray, recorded NOWHERE, which is a
     * different event from "recorded, not complete yet" and deserves its own log line.
     * {@code securitySagaId} is the handle the verdict must echo (null when the saga has none), and
     * {@code policy} is the leaver's stored choices, which the CLOSURE command carries back to the
     * participants — they apply their rule at erasure time, so the last confirmation is exactly
     * when that policy is needed again.
     */
    record Recorded(UUID sagaId, UUID securitySagaId, boolean completedSaga, String policy) {

        /** The pre-closure spelling, for callers and tests that have no policy to carry. */
        public Recorded(UUID sagaId, UUID securitySagaId, boolean completedSaga) {
            this(sagaId, securitySagaId, completedSaga, null);
        }
    }

    /**
     * One overdue saga the sweeper re-commands instead of giving up on. {@code policy} is the
     * leaver's choices as stored at start — the verbatim JSON object, or null when the fact
     * carried none — so the re-command can repeat the original command instead of a bare default.
     */
    record Retry(UUID sagaId, String email, String policy) {
        public Retry(UUID sagaId, String email) {
            this(sagaId, email, null);
        }
    }

    /**
     * One saga the sweeper gave up on — with the participants that DID confirm, so the failure
     * announcement can say which content is already gone (a partial purge is not a no-op), and
     * with security's handle on the deletion, which the failure verdict echoes.
     */
    record Compensated(UUID sagaId, String email, Set<String> confirmed, UUID securitySagaId) {
        public Compensated(UUID sagaId, String email, Set<String> confirmed) {
            this(sagaId, email, confirmed, null);
        }
    }

    /** What one sweep pass decided about the overdue sagas. */
    record SweepResult(List<Retry> retries, List<Compensated> compensated) {
    }

    /**
     * A finished saga whose outcome never got its announced mark — the outbox backlog. The state
     * says which outcome to (re-)publish; {@code confirmed} matters only for the failed ones;
     * {@code securitySagaId} is echoed by the re-published verdict just like by the first one; and
     * {@code policy} is what the accompanying CLOSURE command carries, because the participants'
     * command and the outcome are published — and withheld — together.
     */
    record PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed,
                          UUID securitySagaId, String policy) {
        public PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed) {
            this(sagaId, email, state, confirmed, null, null);
        }

        public PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed,
                              UUID securitySagaId) {
            this(sagaId, email, state, confirmed, securitySagaId, null);
        }
    }

    /**
     * Start a saga for this opening — or return the saga this exact fact already opened (a replayed
     * fact, even after completion), or the one already running for the email (a second request
     * racing the first). Only a genuinely new fact for an email with no running saga starts fresh.
     * {@code policy} is the leaver's choices off the fact, an opaque JSON object stored verbatim
     * (or null) — persisted so the sweeper's re-command can repeat them ({@link Retry#policy});
     * a fact that merely joins an existing saga does not overwrite what the opening fact chose.
     * Security's handle is the one exception: a joining fact comes from a NEWER security saga (the
     * previous one must have finished for security to ask again), and that newer saga is the one
     * waiting for the verdict — so the handle is adopted while the policy and participants stay
     * the opening fact's.
     */
    UUID start(Opening opening, Instant at);

    /** Start with the pre-Opening spelling: no security handle, no recorded participant set. */
    default UUID start(UUID factId, String email, String policy, Instant at) {
        return start(new Opening(factId, email, policy, null, null), at);
    }

    /** Start without policy choices — the pre-policy spelling, kept for callers and tests. */
    default UUID start(UUID factId, String email, Instant at) {
        return start(factId, email, null, at);
    }

    /**
     * Record one participant's confirmation. Fresh confirmations echo the saga id the command
     * carried — that is the precise address; without one (old producers) the running saga for the
     * email is the fallback. A confirmation with no saga to land on is a stray: it records nothing
     * and returns {@link Optional#empty()}. {@code required} is the caller's CONFIGURATION, used
     * only for sagas that recorded no participant set of their own ({@link Opening#participants});
     * a saga that recorded one is judged against THAT set, so re-configuring the participants can
     * never change the completeness criterion of a case already under way.
     */
    Optional<Recorded> confirm(String email, UUID sagaId, String participant, Set<String> required,
                               Instant at);

    /** STARTED straight to COMPLETED (no participants required); true only for the call that did it. */
    boolean complete(String email, Instant at);

    /**
     * STARTED and UNTOUCHED since before the cutoff: re-command while retries remain, COMPENSATED
     * only once they are exhausted. "Untouched" is the whole point of the arithmetic: the clock
     * runs from the LAST delivered re-command ({@link #retryDelivered} moves it), not from the
     * saga's birth. Measured from birth, a saga past the deadline came back as a candidate on
     * EVERY pass — three retries in three sweep intervals (45s at a 15s cadence) — and the
     * verdict went out while the participant still had a live retry budget for the re-command it
     * had just been sent: the purge then ran AFTER the failure was announced, leaving the leaver
     * with an account, an apology, and no content.
     *
     * <p>Returning a {@link Retry} does NOT touch the counter — an attempt counts only once the
     * re-commanded purge demonstrably reached the broker, which the caller reports via
     * {@link #retryDelivered} (the same delivered-first discipline as the outcome outbox). A dead
     * broker can therefore never burn the retries: every sweep hands back the same candidates
     * until one of the re-commands actually lands. Sweeping twice moves nothing twice.
     */
    SweepResult sweepOverdue(Instant cutoff, int maxRetries, Instant at);

    /**
     * The retry counter's second half: the re-commanded purge reached the broker, so the attempt
     * counts against maxRetries now — and only now. A no-op unless the saga is still STARTED.
     * It also stamps {@code at} on the saga, which is what gives the participant its budget: the
     * overdue clock of {@link #sweepOverdue} restarts from this moment, so the next re-command —
     * or the capitulation — cannot happen before the whole purge timeout has passed since the
     * participant was last asked. The stamp is the CALLER's instant, from the same clock the
     * cutoff is derived from; a second clock (the database's) would put skew straight into that
     * budget. Returns whether the counter actually moved — false for the no-op on a finished or
     * unknown saga — so the caller's retries-delivered metric counts charges, never no-ops.
     */
    boolean retryDelivered(UUID sagaId, Instant at);

    /** The outbox's second half: the saga's outcome reached the broker, remember that. */
    void markAnnounced(UUID sagaId);

    /**
     * Finished sagas still owing their outcome, untouched since before {@code olderThan} — the
     * age guard keeps the sweeper from double-publishing outcomes that are merely in flight.
     */
    List<PendingOutcome> unannouncedOutcomes(Instant olderThan);

    /**
     * PII retention: drop finished-and-announced sagas (and their confirmations) untouched since
     * before {@code olderThan}; returns how many sagas went. Deleting a saga also forgets its
     * fact_id, so a replay of a very old fact would fork a fresh saga for an account that no
     * longer exists — accepted, because Kafka's upstream retention is far shorter than this
     * threshold and such a replay cannot reach us in practice.
     */
    int deleteFinishedBefore(Instant olderThan);
}
