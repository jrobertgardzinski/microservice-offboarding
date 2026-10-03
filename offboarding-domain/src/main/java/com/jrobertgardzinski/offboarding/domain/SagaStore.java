package com.jrobertgardzinski.offboarding.domain;

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
        return start(factId, email, (String) null, at);
    }

    /** Test seeding: a saga that knows its leaver's id. */
    default UUID start(UUID factId, String email, UUID userId, Instant at) {
        return start(new Opening(factId, email, null, null, null, null, userId), at);
    }

    /**
     * Record one participant's confirmation. Fresh confirmations echo the saga id the command
     * carried — that is the precise address; without one (old producers) the running saga of
     * {@code userId} is the fallback. A confirmation with no saga to land on is a stray: it
     * records nothing and returns {@link Optional#empty()}. {@code required} is the caller's
     * CONFIGURATION, used only for sagas that recorded no participant set of their own
     * ({@link Opening#participants}); a saga that recorded one is judged against THAT set, so
     * re-configuring the participants can never change the completeness criterion of a case
     * already under way.
     */
    Optional<Recorded> confirm(UUID userId, UUID sagaId, String participant, Set<String> required,
                               Instant at);

    /**
     * STARTED straight to COMPLETED for the saga NAMED — the case with nobody to wait for; true
     * only for the call that did it.
     *
     * <p>It takes the saga id and not the address, because an address is a person and a person can
     * ask twice. Addressed by email this settled whatever saga happened to be RUNNING for that
     * account, which is not the same case at all: a replayed fact whose own saga finished long ago
     * would force-complete the deletion running for that person now.
     *
     * <p>And the named saga's own quorum has the last word: a saga that recorded participants
     * (V6) is never completed here, whatever the caller's configuration says now. Zero
     * participants is the CALLER's present state, while the quorum on the row is what this case
     * opened with — completing across that difference is precisely the re-configuration V6 exists
     * to keep out of cases already under way, and it would announce the portal purged with no
     * confirmation on file. A row that recorded nothing (NULL — opened before V6) still defers to
     * the caller, exactly as it does for {@link #confirm}.
     */
    boolean complete(UUID sagaId, Instant at);

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
     *
     * <p>{@code retriesSoFar} is the counter the sweep handed out with the candidate
     * ({@link Retry#retriesSoFar}), and the charge only applies while the row still reads that
     * way. An increment with no such condition is one charge PER SWEEPER: nothing stops two
     * sweeps (two pods mid-rollout, say) from selecting the same overdue saga and both reporting
     * their re-command delivered, and the counter then runs 0 → 2 in a round the budget sized for
     * one. Half the retries a case is supposed to get, spent silently, and no later pass can tell
     * the number is wrong. The duplicate COMMAND is free — participants are idempotent — so what
     * has to be exactly-once is the charge, not the send.
     */
    boolean retryDelivered(UUID sagaId, int retriesSoFar, Instant at);

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
