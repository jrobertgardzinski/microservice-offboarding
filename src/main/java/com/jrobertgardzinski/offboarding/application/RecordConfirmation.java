package com.jrobertgardzinski.offboarding.application;

import java.time.Instant;
import java.util.Set;

/**
 * One content participant confirmed its purge. True only when that confirmation was the last
 * required one — the caller then announces the portal purged. Duplicates and strays are no-ops.
 */
public class RecordConfirmation {

    private final SagaStore sagas;
    private final Set<String> participants;

    public RecordConfirmation(SagaStore sagas, Set<String> participants) {
        this.sagas = sagas;
        this.participants = participants;
    }

    public boolean execute(String email, String participant, Instant at) {
        return sagas.confirm(email, participant, participants, at);
    }
}
