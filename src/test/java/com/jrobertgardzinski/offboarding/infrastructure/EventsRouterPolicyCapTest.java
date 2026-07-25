package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.SagaStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The policy size cap ({@link EventsRouter#MAX_POLICY_BYTES}): the router only ferries the
 * leaver's choices, so an absurdly large policy object must not ride into the saga row and every
 * re-command — past the cap the saga starts WITHOUT it (the participants' defaults), exactly like
 * an unreadable stored policy, while a policy within the cap keeps riding verbatim. Either way
 * the saga itself must open: the cap drops the baggage, never the deletion.
 */
class EventsRouterPolicyCapTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void an_oversized_policy_is_dropped_from_the_command_and_the_saga_alike() throws Exception {
        RouterFixture fixture = RouterFixture.router();
        String blob = "x".repeat(EventsRouter.MAX_POLICY_BYTES + 1);
        String fact = "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                + "\"email\":\"alice@example.com\",\"version\":1,\"policy\":{\"note\":\"" + blob + "\"}}";

        List<EventsRouter.Outgoing> out = fixture.router.handle(RouterFixture.FACTS_TOPIC, fact);

        assertEquals(1, out.size(), "the saga must still open — the cap drops the baggage,"
                + " never the deletion");
        JsonNode command = MAPPER.readTree(out.getFirst().payload());
        assertEquals("PURGE_USER_CONTENT", command.path("type").asText());
        assertFalse(command.has("policy"),
                "an over-cap policy must not ride the command");
        // and the saga stored none either: the sweeper's retry candidate carries a null policy,
        // so the re-command stays identical to the original (both on the participants' defaults)
        SagaStore.SweepResult swept = fixture.store.sweepOverdue(
                Instant.parse("2026-07-11T12:01:00Z"), 3, Instant.parse("2026-07-11T12:01:00Z"));
        assertEquals(1, swept.retries().size());
        assertEquals(null, swept.retries().getFirst().policy(),
                "an over-cap policy must not be stored with the saga");
    }

    @Test
    void a_policy_within_the_cap_still_rides_and_is_stored_verbatim() throws Exception {
        RouterFixture fixture = RouterFixture.router();
        String fact = "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                + "\"email\":\"alice@example.com\",\"version\":1,"
                + "\"policy\":{\"memes\":\"DELETE_ALL\"}}";

        List<EventsRouter.Outgoing> out = fixture.router.handle(RouterFixture.FACTS_TOPIC, fact);

        JsonNode command = MAPPER.readTree(out.getFirst().payload());
        assertEquals("DELETE_ALL", command.path("policy").path("memes").asText(),
                "a modest policy keeps riding the command verbatim");
        SagaStore.SweepResult swept = fixture.store.sweepOverdue(
                Instant.parse("2026-07-11T12:01:00Z"), 3, Instant.parse("2026-07-11T12:01:00Z"));
        assertEquals("{\"memes\":\"DELETE_ALL\"}", swept.retries().getFirst().policy(),
                "and is stored with the saga for the sweeper's re-command");
    }

    @Test
    void the_cap_counts_utf8_bytes_not_characters() throws Exception {
        // a multi-byte payload: fewer CHARACTERS than the cap, more BYTES — the cap guards
        // storage and the wire, both of which count bytes
        RouterFixture fixture = RouterFixture.router();
        String multiByte = "ż".repeat(EventsRouter.MAX_POLICY_BYTES / 2 + 100);
        assertTrue(multiByte.length() < EventsRouter.MAX_POLICY_BYTES);
        assertTrue(multiByte.getBytes(StandardCharsets.UTF_8).length > EventsRouter.MAX_POLICY_BYTES);
        String fact = "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                + "\"email\":\"alice@example.com\",\"version\":1,\"policy\":{\"note\":\""
                + multiByte + "\"}}";

        List<EventsRouter.Outgoing> out = fixture.router.handle(RouterFixture.FACTS_TOPIC, fact);

        assertEquals(1, out.size());
        assertFalse(MAPPER.readTree(out.getFirst().payload()).has("policy"),
                "the byte-counted cap must catch a multi-byte blob too");
    }
}
