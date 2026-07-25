package com.jrobertgardzinski.offboarding.infrastructure;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static com.jrobertgardzinski.offboarding.infrastructure.RouterFixture.router;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Poison pills must drop, never throw: with at-least-once delivery an exception would park the
 * loop on the same bad record forever, wedging the whole topic behind it. Records the router
 * cannot possibly route — a fact without a usable replay key, anything without an email — go the
 * way of malformed JSON: one WARN line, no saga touched, life goes on.
 */
class EventsRouterPoisonPillTest {

    @Test
    void a_fact_without_an_id_drops_without_a_saga() {
        RouterFixture fixture = router();
        List<EventsRouter.Outgoing> out = fixture.router.handle(RouterFixture.FACTS_TOPIC,
                "{\"type\":\"ACCOUNT_DELETION_REQUESTED\",\"email\":\"leaver@example.com\",\"version\":1}");
        assertEquals(List.of(), out, "no command may go out for an unreplayable fact");
        assertEquals(List.of(), fixture.store.all(), "and no saga may open");
    }

    @Test
    void a_fact_with_a_mangled_id_drops_without_a_saga() {
        RouterFixture fixture = router();
        List<EventsRouter.Outgoing> out = fixture.router.handle(RouterFixture.FACTS_TOPIC,
                "{\"id\":\"definitely-not-a-uuid\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");
        assertEquals(List.of(), out);
        assertEquals(List.of(), fixture.store.all());
    }

    @Test
    void a_fact_without_an_email_drops_without_a_saga() {
        RouterFixture fixture = router();
        List<EventsRouter.Outgoing> out = fixture.router.handle(RouterFixture.FACTS_TOPIC,
                "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\",\"version\":1}");
        assertEquals(List.of(), out);
        assertEquals(List.of(), fixture.store.all());
    }

    @Test
    void a_confirmation_without_an_email_drops_and_confirms_nothing() {
        RouterFixture fixture = router().withRunningSagaFor("leaver@example.com");
        List<EventsRouter.Outgoing> out = fixture.router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"version\":1}");
        assertEquals(List.of(), out);
        assertEquals(List.of(), List.copyOf(fixture.store.all().get(0).confirmed),
                "the running saga must stay untouched");
    }

    @Test
    void a_confirmation_with_a_blank_email_drops_and_confirms_nothing() {
        RouterFixture fixture = router().withRunningSagaFor("leaver@example.com");
        fixture.router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"\",\"version\":1}");
        assertTrue(fixture.store.all().get(0).confirmed.isEmpty());
    }

    @Test
    void a_confirmation_with_a_mangled_sagaId_drops_and_confirms_nothing() {
        // a PRESENT-but-unparseable sagaId is a poison pill, not an invitation to guess: falling
        // back to the email lookup would let a mangled echo of a CLOSED case land on a newer saga
        // for the same account — the very hole the precise address exists to close
        RouterFixture fixture = router().withRunningSagaFor("leaver@example.com");
        List<EventsRouter.Outgoing> out = fixture.router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"sagaId\":\"garbage\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");
        assertEquals(List.of(), out);
        assertTrue(fixture.store.all().get(0).confirmed.isEmpty(),
                "a mangled address must not confirm ANY saga, not even the email's running one");
    }

    @Test
    void a_confirmation_with_an_explicit_null_sagaId_drops_and_confirms_nothing() {
        // "sagaId": null is a PRESENT field whose value the producer failed to fill — a mangled
        // echo, not an old producer that never learned the field. It must drop like the garbage
        // spelling above; degrading it to the email lookup would reopen the same back door
        RouterFixture fixture = router().withRunningSagaFor("leaver@example.com");
        List<EventsRouter.Outgoing> out = fixture.router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"sagaId\":null,"
                        + "\"email\":\"leaver@example.com\",\"version\":1}");
        assertEquals(List.of(), out);
        assertTrue(fixture.store.all().get(0).confirmed.isEmpty(),
                "an explicit null sagaId must not degrade to the email's running saga");
    }

    @Test
    void a_confirmation_without_the_sagaId_field_lands_via_the_email_fallback() {
        // the fallback exists solely for old producers that never learned the field — ABSENT is
        // the only spelling that may degrade to the email lookup
        RouterFixture fixture = router().withRunningSagaFor("leaver@example.com");
        fixture.router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"leaver@example.com\",\"version\":1}");
        assertTrue(fixture.store.all().get(0).confirmed.contains("memes"));
    }

    @Test
    void a_confirmation_echoing_the_sagas_own_id_lands_precisely() {
        RouterFixture fixture = router().withRunningSagaFor("leaver@example.com");
        UUID sagaId = fixture.store.all().get(0).id;
        fixture.router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"sagaId\":\"" + sagaId + "\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");
        assertTrue(fixture.store.all().get(0).confirmed.contains("memes"));
    }
}
