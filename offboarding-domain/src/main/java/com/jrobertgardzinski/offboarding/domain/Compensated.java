package com.jrobertgardzinski.offboarding.domain;

import java.util.Set;
import java.util.UUID;

/**
 * One saga the sweeper gave up on — with the participants that DID confirm, so the failure
 * announcement can say which content is already gone (a partial purge is not a no-op), and
 * with security's handle on the deletion, which the failure verdict echoes.
 */
public record Compensated(UUID sagaId, String email, Set<String> confirmed, UUID securitySagaId,
                          UUID userId) {
    public Compensated(UUID sagaId, String email, Set<String> confirmed, UUID securitySagaId) {
        this(sagaId, email, confirmed, securitySagaId, null);
    }

    public Compensated(UUID sagaId, String email, Set<String> confirmed) {
        this(sagaId, email, confirmed, null);
    }
}
