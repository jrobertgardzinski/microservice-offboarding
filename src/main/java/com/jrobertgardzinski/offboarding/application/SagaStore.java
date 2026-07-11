package com.jrobertgardzinski.offboarding.application;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Persistence of the portal-side account-deletion saga: STARTED when security announces the
 * deletion fact; one confirmation per content participant; COMPLETED the moment the LAST required
 * participant confirmed; COMPENSATED when confirmations never came in time. The required set is
 * the caller's CONFIGURATION — the store records confirmations by name and never hardcodes who
 * participates. Transitions are idempotent — at-least-once delivery makes duplicates a fact of
 * life — and the STARTED→COMPLETED update is the once-latch: exactly one call learns it completed.
 */
public interface SagaStore {

    /**
     * Start a saga for this email — or return the saga this exact fact already opened (a replayed
     * fact, even after completion), or the one already running for the email (a second request
     * racing the first). Only a genuinely new fact for an email with no running saga starts fresh.
     */
    UUID start(UUID factId, String email, Instant at);

    /**
     * Record one participant's confirmation for this email's running saga; true only when this
     * confirmation was the last required one and the saga just COMPLETED. A confirmation with no
     * running saga is a stray and records nothing.
     */
    boolean confirm(String email, String participant, Set<String> required, Instant at);

    /** STARTED straight to COMPLETED (no participants required); true only for the call that did it. */
    boolean complete(String email, Instant at);

    /** STARTED older than the cutoff → COMPENSATED; returns the affected emails. */
    List<String> compensateOverdue(Instant cutoff, Instant at);
}
