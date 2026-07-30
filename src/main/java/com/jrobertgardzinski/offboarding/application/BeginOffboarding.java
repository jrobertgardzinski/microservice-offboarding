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

    /**
     * What begin decided. {@code nothingToPurge} says there is nobody to command — the portal is
     * clean by configuration. {@code completedNow} is the once-latch's answer: true only when THIS
     * call turned the saga COMPLETED, so only this call may announce the outcome. The two differ
     * on a replayed fact for a portal without participants: there is still nothing to purge, but
     * the saga finished long ago and announcing again would put a second verdict on the wire (only
     * the deterministic outcome id keeps consumers from acting on it twice).
     */
    public record Begun(UUID sagaId, boolean nothingToPurge, boolean completedNow) {
    }

    private final SagaStore sagas;
    private final Set<String> participants;

    public BeginOffboarding(SagaStore sagas, Set<String> participants) {
        this.sagas = sagas;
        this.participants = participants;
    }

    public Begun execute(UUID factId, String email, Instant at) {
        return execute(factId, email, null, at);
    }

    public Begun execute(UUID factId, String email, String policy, Instant at) {
        return execute(factId, email, policy, null, at);
    }

    /**
     * {@code policy} is the leaver's choices off the fact — an opaque JSON object, stored with
     * the saga so the sweeper's re-command can repeat the original command instead of sending the
     * participants back to their defaults. {@code securitySagaId} is security's own handle on the
     * deletion, stored so the verdict can echo it back to the saga that is waiting for it. The
     * participant set is stored WITH the saga: it is the quorum this case must reach, and
     * re-configuring the participants must not change the criterion for cases already open.
     */
    public Begun execute(UUID factId, String email, String policy, UUID securitySagaId, Instant at) {
        UUID sagaId = sagas.start(
                new SagaStore.Opening(factId, email, policy, securitySagaId, participants), at);
        if (participants.isEmpty()) {
            return new Begun(sagaId, true, sagas.complete(email, at));
        }
        return new Begun(sagaId, false, false);
    }
}
