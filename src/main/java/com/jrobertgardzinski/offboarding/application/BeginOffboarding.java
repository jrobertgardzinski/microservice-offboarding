package com.jrobertgardzinski.offboarding.application;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Security announced the FACT that an account requested deletion; this use case opens the portal's
 * purge saga. With participants configured, the caller sends them the purge command next; with an
 * empty participant set (a portal with no content services — or a rehearsal deployment) the portal
 * is instantly clean and the saga completes on the spot.
 */
public class BeginOffboarding {

    /** What begin decided: the saga to command under, and whether there is nobody to command. */
    public record Begun(UUID sagaId, boolean completedImmediately) {
    }

    private final SagaStore sagas;
    private final Set<String> participants;

    public BeginOffboarding(SagaStore sagas, Set<String> participants) {
        this.sagas = sagas;
        this.participants = participants;
    }

    public Begun execute(UUID factId, String email, Instant at) {
        UUID sagaId = sagas.start(factId, email, at);
        if (participants.isEmpty()) {
            sagas.complete(email, at);
            return new Begun(sagaId, true);
        }
        return new Begun(sagaId, false);
    }
}
