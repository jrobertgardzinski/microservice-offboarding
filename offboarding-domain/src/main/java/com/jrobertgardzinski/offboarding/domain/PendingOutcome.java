package com.jrobertgardzinski.offboarding.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * A finished saga whose outcome never got its announced mark — the outbox backlog. The state
 * says which outcome to (re-)publish; {@code confirmed} matters only for the failed ones;
 * {@code securitySagaId} is echoed by the re-published verdict just like by the first one; and
 * {@code policy} is what the accompanying CLOSURE command carries, because the participants'
 * command and the outcome are published — and withheld — together.
 */
public record PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed,
                      UUID securitySagaId, String policy, String initiatedBy, UUID userId) {
    public PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed,
                          UUID securitySagaId, String policy, String initiatedBy) {
        this(sagaId, email, state, confirmed, securitySagaId, policy, initiatedBy, null);
    }

    public PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed) {
        this(sagaId, email, state, confirmed, null, null, null);
    }

    public PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed,
                          UUID securitySagaId) {
        this(sagaId, email, state, confirmed, securitySagaId, null, null);
    }

    public PendingOutcome(UUID sagaId, String email, String state, Set<String> confirmed,
                          UUID securitySagaId, String policy) {
        this(sagaId, email, state, confirmed, securitySagaId, policy, null);
    }
}
