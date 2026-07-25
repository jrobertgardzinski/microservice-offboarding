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

    /** One overdue saga the sweeper re-commands instead of giving up on. */
    record Retry(UUID sagaId, String email) {
    }

    /**
     * One saga the sweeper gave up on — with the participants that DID confirm, so the failure
     * announcement can say which content is already gone (a partial purge is not a no-op).
     */
    record Compensated(UUID sagaId, String email, Set<String> confirmed) {
    }

    /** What one sweep pass decided about the overdue sagas. */
    record SweepResult(List<Retry> retries, List<Compensated> compensated) {
    }

    /**
     * A finished saga whose outcome never got its announced mark — the outbox backlog. The state
     * says which outcome to (re-)publish; {@code confirmed} matters only for the failed ones.
     */
    record PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed) {
    }

    /**
     * Start a saga for this email — or return the saga this exact fact already opened (a replayed
     * fact, even after completion), or the one already running for the email (a second request
     * racing the first). Only a genuinely new fact for an email with no running saga starts fresh.
     */
    UUID start(UUID factId, String email, Instant at);

    /**
     * Record one participant's confirmation. Fresh confirmations echo the saga id the command
     * carried — that is the precise address; without one (old producers) the running saga for the
     * email is the fallback. Returns the saga id ONLY when this confirmation was the last required
     * one and the saga just COMPLETED. A confirmation with no saga to land on is a stray and
     * records nothing.
     */
    Optional<UUID> confirm(String email, UUID sagaId, String participant, Set<String> required, Instant at);

    /** STARTED straight to COMPLETED (no participants required); true only for the call that did it. */
    boolean complete(String email, Instant at);

    /**
     * STARTED older than the cutoff: re-command while retries remain (incrementing the counter),
     * COMPENSATED only once they are exhausted. Sweeping twice moves nothing twice.
     */
    SweepResult sweepOverdue(Instant cutoff, int maxRetries, Instant at);

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
