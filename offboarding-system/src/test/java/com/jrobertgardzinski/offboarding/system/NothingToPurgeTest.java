package com.jrobertgardzinski.offboarding.system;

import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The "nobody to command" path, and whose case it is allowed to close. A portal with no content
 * participants is clean the moment a deletion is announced — but only the saga THIS fact opened is
 * clean, and only while that saga waits for nobody either. An address is a person, and a person
 * can ask twice.
 */
@Epic("Use case")
@Feature("Begin offboarding")
class NothingToPurgeTest {

    private static final String ALICE = "alice@example.com";
    private static final Instant T0 = Instant.parse("2026-07-11T12:00:00Z");
    private static final Set<String> THREE = Set.of("memes", "comments", "collections");

    @Test
    void a_replayed_fact_never_completes_the_deletion_that_is_running_now() {
        // THE finding this pins. The completion used to be addressed by EMAIL: whatever saga was
        // RUNNING for the address got flipped to COMPLETED, however many participants it was
        // waiting for. A replay of an old fact — at-least-once delivery makes that routine —
        // therefore closed the case that had just opened, the portal announced it purged, security
        // deleted the account and no participant was ever told to erase anything.
        FakeSagaStore store = new FakeSagaStore();
        BeginOffboarding withoutParticipants = new BeginOffboarding(store, Set.of());
        UUID firstFact = UUID.randomUUID();

        BeginOffboarding.Begun first = withoutParticipants.execute(firstFact, ALICE, T0);
        assertTrue(first.completedNow(), "nobody to command: this case really is clean");

        // the content services are back, and alice asks again — a NEW case, with a real quorum
        BeginOffboarding.Begun current = new BeginOffboarding(store, THREE)
                .execute(UUID.randomUUID(), ALICE, T0.plusSeconds(10));
        assertFalse(current.nothingToPurge());

        BeginOffboarding.Begun replay = withoutParticipants.execute(firstFact, ALICE,
                T0.plusSeconds(20));

        assertEquals(first.sagaId(), replay.sagaId(), "the replay belongs to the saga it opened");
        assertFalse(replay.completedNow(),
                "and settles nothing: its own saga finished long ago, and the case running now is"
                        + " not this fact's to close");
        assertEquals("STARTED", state(store, current.sagaId()),
                "the deletion that is actually under way must still be collecting its three"
                        + " confirmations — completing it here announces the portal purged with"
                        + " nothing purged at all");
    }

    @Test
    void a_deployment_without_participants_cannot_close_a_case_that_recorded_three() {
        // the other half: the saga is not a replay at all, it is the running case this fact JOINS.
        // Its quorum was recorded when it opened (V6), and a pod that came up with an empty
        // participant set may not redefine completeness for a case already under way
        FakeSagaStore store = new FakeSagaStore();
        BeginOffboarding.Begun underWay = new BeginOffboarding(store, THREE)
                .execute(UUID.randomUUID(), ALICE, T0);

        BeginOffboarding.Begun joined = new BeginOffboarding(store, Set.of())
                .execute(UUID.randomUUID(), ALICE, T0.plusSeconds(5));

        assertEquals(underWay.sagaId(), joined.sagaId(), "one running saga per account");
        assertFalse(joined.completedNow(), "the recorded quorum has the last word");
        assertEquals("STARTED", state(store, underWay.sagaId()));
    }

    private static String state(FakeSagaStore store, UUID sagaId) {
        return store.all().stream().filter(saga -> saga.id.equals(sagaId)).findFirst()
                .orElseThrow().state;
    }
}
