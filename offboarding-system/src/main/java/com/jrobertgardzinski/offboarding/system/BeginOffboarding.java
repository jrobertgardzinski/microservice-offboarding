package com.jrobertgardzinski.offboarding.system;

import com.jrobertgardzinski.offboarding.domain.Opening;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * Security announced the FACT that an account requested deletion; this use case opens the portal's
 * purge saga. With participants configured, the caller sends them the purge command next; with an
 * empty participant set there is nobody to command, so the portal is clean and the saga completes
 * on the spot — for the saga THIS call opened, and only while that saga waits for nobody either.
 * The deployable refuses to boot with an empty set (a blank environment variable and a deliberate
 * "no content services" are spelled the same), so that path belongs to an assembly that hands this
 * use case its participants directly.
 */
public class BeginOffboarding {

    /**
     * What begin decided. {@code nothingToPurge} says there is nobody to command — the portal is
     * clean by configuration. {@code completedNow} is the once-latch's answer: true only when THIS
     * call turned the saga COMPLETED, so only this call may announce the outcome. The two differ
     * on a replayed fact for a portal without participants: there is still nothing to purge, but
     * the saga finished long ago and announcing again would put a second verdict on the wire (only
     * the deterministic outcome id keeps consumers from acting on it twice). They differ for one
     * other reason too — the saga this fact joined recorded a quorum of its own, which no
     * participant-less caller may declare reached.
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
        return execute(factId, email, policy, securitySagaId, null, at);
    }

    /**
     * {@code initiatedBy} is the basis the closure was requested under — the data subject's own
     * right, or an administrator's decision. It is stored with the saga for the same reason the
     * policy is: the closure command that carries it is often built from this row alone, long
     * after the fact has left the topic.
     */
    public Begun execute(UUID factId, String email, String policy, UUID securitySagaId,
                         String initiatedBy, Instant at) {
        return execute(factId, email, policy, securitySagaId, initiatedBy, null, at);
    }

    /** {@code userId} is the leaver's identity as security stated it, or null on an older fact. */
    public Begun execute(UUID factId, String email, String policy, UUID securitySagaId,
                         String initiatedBy, UUID userId, Instant at) {
        UUID sagaId = sagas.start(new Opening(factId, email, policy, securitySagaId,
                participants, initiatedBy, userId), at);
        if (participants.isEmpty()) {
            // the saga this call just opened or joined, BY ID: complete(email) settled whichever
            // saga was running for the address, so a replayed fact whose own saga finished could
            // force-complete the case running for that person now. The store also refuses a saga
            // that recorded a quorum of its own, so a deployment that lost its participants cannot
            // declare a case with three of them clean
            return new Begun(sagaId, true, sagas.complete(sagaId, at));
        }
        return new Begun(sagaId, false, false);
    }
}
