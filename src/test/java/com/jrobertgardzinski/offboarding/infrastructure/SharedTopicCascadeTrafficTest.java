package com.jrobertgardzinski.offboarding.infrastructure;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static com.jrobertgardzinski.offboarding.infrastructure.RouterFixture.router;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The participants' topics are NOT the saga's private line, and since the deletion cascade shipped
 * they carry a second conversation entirely: microservice-memes announces {@code MEME_DELETED} on
 * {@code memes-events} and microservice-comments answers it with {@code COMMENTS_DELETED} on
 * {@code comments-events} — the same topic this service reads its purge confirmations from.
 *
 * <p>Nothing in that conversation is addressed to the saga. The router already reads on
 * {@code (topic, type)} and answers a type it does not know with silence, so the cascade is
 * invisible to it BY CONSTRUCTION — this test is here so it stays that way. The failure it guards
 * against is not hypothetical: a "tolerant" widening of the confirmation branch (matching any type
 * on a participant topic, or falling back to the email lookup for a type it cannot place) would
 * make a stranger's announcement confirm a participant that never purged, and the saga would
 * announce PORTAL_CONTENT_PURGED over content that is still there — a false GDPR all-clear.
 *
 * <p>The payloads below are the producers' real envelopes, copied field for field from
 * microservice-comments' {@code KafkaCommentEvents} and microservice-memes' {@code KafkaMemeEvents}
 * (the shape is pinned on their side by their own tests; here only its FATE is pinned).
 */
class SharedTopicCascadeTrafficTest {

    private static final String LEAVER = "leaver@example.com";

    /** microservice-comments' COMMENTS_DELETED, verbatim: no email, no sagaId, its own type. */
    private static String commentsDeleted(String memeId) {
        return "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"COMMENTS_DELETED\",\"memeId\":\""
                + memeId + "\",\"commentIds\":[\"" + UUID.randomUUID() + "\",\"" + UUID.randomUUID()
                + "\"],\"version\":1}";
    }

    /** microservice-memes' MEME_DELETED, verbatim — the hop that starts the cascade. */
    private static String memeDeleted(String memeId) {
        return "{\"type\":\"MEME_DELETED\",\"memeId\":\"" + memeId + "\",\"eventId\":\""
                + UUID.randomUUID() + "\"}";
    }

    @Test
    @DisplayName("a COMMENTS_DELETED on the confirmations' topic confirms nothing and says nothing")
    void the_cascade_announcement_is_not_a_confirmation() {
        RouterFixture fixture = router().withRunningSagaFor(LEAVER);

        List<EventsRouter.Outgoing> out =
                fixture.router.handle("comments-events", commentsDeleted(UUID.randomUUID().toString()));

        assertEquals(List.of(), out, "the cascade's announcement is not addressed to the saga");
        InMemorySagaStore.Saga saga = fixture.store.all().get(0);
        assertTrue(saga.confirmed.isEmpty(),
                "comments announced a dropped THREAD, not a purged ACCOUNT — nothing may be "
                        + "recorded against the saga");
        assertEquals("STARTED", saga.state, "and the saga may not advance an inch");
    }

    @Test
    @DisplayName("nor does the hop before it, MEME_DELETED on the memes participant's topic")
    void the_first_cascade_hop_is_not_a_confirmation_either() {
        RouterFixture fixture = router().withRunningSagaFor(LEAVER);

        List<EventsRouter.Outgoing> out =
                fixture.router.handle("memes-events", memeDeleted(UUID.randomUUID().toString()));

        assertEquals(List.of(), out);
        assertTrue(fixture.store.all().get(0).confirmed.isEmpty());
        assertEquals("STARTED", fixture.store.all().get(0).state);
    }

    @Test
    @DisplayName("a whole cascade cannot close a saga that only one participant has confirmed")
    void the_cascade_cannot_complete_a_saga_by_the_back_door() {
        RouterFixture fixture = router().withRunningSagaFor(LEAVER);
        UUID sagaId = fixture.store.all().get(0).id;
        // the genuine article from ONE participant; two are still owed
        fixture.router.handle("memes-events", confirmation(sagaId));

        // ...and then the cascade's traffic on both of the topics the missing confirmations
        // would arrive on. If any of it counted, the saga would close two participants short
        String memeId = UUID.randomUUID().toString();
        fixture.router.handle("memes-events", memeDeleted(memeId));
        List<EventsRouter.Outgoing> out = fixture.router.handle("comments-events", commentsDeleted(memeId));

        assertEquals(List.of(), out, "no outcome may be announced while two participants are owed");
        assertEquals(java.util.Set.of("memes"), fixture.store.all().get(0).confirmed,
                "only the participant that actually purged may appear");
        assertEquals("STARTED", fixture.store.all().get(0).state);
    }

    @Test
    @DisplayName("and the cascade's traffic does not get in the confirmation's way")
    void a_real_confirmation_still_lands_behind_the_cascade() {
        RouterFixture fixture = router().withRunningSagaFor(LEAVER);
        UUID sagaId = fixture.store.all().get(0).id;

        // interleaved exactly as the broker would deliver it: a meme deleted, its thread dropped,
        // and only then the confirmation of the account purge that has nothing to do with either
        fixture.router.handle("comments-events", commentsDeleted(UUID.randomUUID().toString()));
        fixture.router.handle("comments-events", confirmation(sagaId));

        assertTrue(fixture.store.all().get(0).confirmed.contains("comments"),
                "the confirmation behind the cascade traffic must still be recorded");
    }

    private static String confirmation(UUID sagaId) {
        return "{\"type\":\"USER_CONTENT_PURGED\",\"sagaId\":\"" + sagaId + "\",\"email\":\""
                + LEAVER + "\",\"version\":1}";
    }
}
