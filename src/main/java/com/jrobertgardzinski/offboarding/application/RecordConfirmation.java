package com.jrobertgardzinski.offboarding.application;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * One content participant confirmed its purge — addressed by the saga id it echoes from the
 * command, or by email for confirmations predating the field. Returns the saga id only when that
 * confirmation was the last required one — the caller then announces the portal purged (and the
 * id is what the outbox marks announced). Duplicates and strays are no-ops.
 */
public class RecordConfirmation {

    private final SagaStore sagas;
    private final Set<String> participants;

    public RecordConfirmation(SagaStore sagas, Set<String> participants) {
        this.sagas = sagas;
        this.participants = participants;
    }

    public Optional<UUID> execute(String email, UUID sagaId, String participant, Instant at) {
        return sagas.confirm(email, sagaId, participant, participants, at);
    }
}
