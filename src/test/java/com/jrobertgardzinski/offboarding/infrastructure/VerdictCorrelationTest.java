package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The verdict's correlation with the deletion REQUEST. Security stamps its own saga id on the
 * {@code ACCOUNT_DELETION_REQUESTED} fact; the portal used to read only id/email/policy, so its
 * {@code PORTAL_CONTENT_PURGED} / {@code PORTAL_PURGE_FAILED} named no request at all and security
 * had to match them BY EMAIL ADDRESS. A verdict of a closed case arriving late then settled
 * whatever deletion was running for that address — a re-announced failure of an abandoned case
 * compensating a NEWER request, unlocking the account precisely while the newer purge erased the
 * content. Every verdict must therefore carry the handle back, on all four paths that emit one.
 *
 * <p>The clock here MOVES, because the sweeper's timeline now depends on it: a delivered
 * re-command buys the participant a whole purge timeout before the next decision.
 */
class VerdictCorrelationTest {

    private static final String FACTS = "security-events";
    private static final Map<String, String> TOPICS = Map.of("memes-events", "memes");
    private static final Duration TIMEOUT = Duration.ofMinutes(2);
    private static final String LEAVER = "leaver@example.com";

    private final ObjectMapper mapper = new ObjectMapper();
    private final InMemorySagaStore store = new InMemorySagaStore();
    private final EventsRouter router;
    private Instant now = Instant.parse("2026-07-11T12:00:00Z");

    VerdictCorrelationTest() {
        Set<String> participants = Set.copyOf(TOPICS.values());
        router = new EventsRouter(FACTS, TOPICS,
                new BeginOffboarding(store, participants),
                new RecordConfirmation(store, participants),
                new SweepOverdue(store, TIMEOUT),
                mapper, movingClock());
    }

    private Clock movingClock() {
        return new Clock() {
            @Override
            public Instant instant() {
                return now;
            }

            @Override
            public ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }
        };
    }

    @Test
    void the_completion_verdict_echoes_the_handle_from_the_fact() throws Exception {
        UUID securitySaga = UUID.randomUUID();
        router.handle(FACTS, fact(UUID.randomUUID(), securitySaga));
        List<EventsRouter.Outgoing> out = router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"" + LEAVER + "\",\"version\":1}");
        assertEquals(securitySaga.toString(), payload(out.get(0)).path("sagaId").asText(),
                "the purged verdict must name the deletion request it settles");
    }

    @Test
    void the_failure_verdict_and_its_republication_echo_the_handle_too() throws Exception {
        UUID securitySaga = UUID.randomUUID();
        router.handle(FACTS, fact(UUID.randomUUID(), securitySaga));
        // silence: one deadline per retry, plus the one that capitulates
        List<EventsRouter.Outgoing> verdicts = List.of();
        for (int deadline = 0; deadline <= SweepOverdue.DEFAULT_MAX_RETRIES; deadline++) {
            verdicts = sweep();
        }
        JsonNode failure = payload(onOutcomes(verdicts));
        assertEquals("PORTAL_PURGE_FAILED", failure.path("type").asText());
        assertEquals(securitySaga.toString(), failure.path("sagaId").asText(),
                "the failure verdict must name the deletion request it compensates");
        // nobody marked it announced, so the outbox owes it — and the re-publication has to be
        // byte-identical, correlation included, or consumers would stop deduplicating on the id
        now = now.plus(SweepOverdue.DEFAULT_REPUBLISH_AFTER).plusSeconds(1);
        assertEquals(failure, payload(onOutcomes(router.sweepOverdue())),
                "a re-announced verdict must carry the same address as the first one");
    }

    @Test
    void the_instantly_clean_verdict_echoes_the_handle_and_is_announced_once() throws Exception {
        // a portal with no content participants: the saga completes on the spot
        EventsRouter bare = new EventsRouter(FACTS, Map.of(),
                new BeginOffboarding(store, Set.of()),
                new RecordConfirmation(store, Set.of()),
                new SweepOverdue(store, TIMEOUT), mapper, movingClock());
        UUID securitySaga = UUID.randomUUID();
        UUID factId = UUID.randomUUID();

        List<EventsRouter.Outgoing> first = bare.handle(FACTS, fact(factId, securitySaga));
        assertEquals(securitySaga.toString(), payload(first.get(0)).path("sagaId").asText(),
                "even the shortcut verdict must name its request");

        // the same fact again (at-least-once delivery). The completion latch already fired, so
        // this replay may NOT put a second verdict on the wire: the store's answer is the whole
        // point of asking it
        now = now.plusSeconds(5);
        assertEquals(List.of(), bare.handle(FACTS, fact(factId, securitySaga)),
                "a replayed fact must not announce the outcome a second time");
    }

    @Test
    void a_fact_without_a_handle_still_opens_a_saga_and_its_verdict_carries_none() throws Exception {
        // an older producer: correlation is defence in depth, not a precondition for deleting
        router.handle(FACTS, "{\"id\":\"" + UUID.randomUUID() + "\","
                + "\"type\":\"ACCOUNT_DELETION_REQUESTED\",\"email\":\"" + LEAVER
                + "\",\"version\":1}");
        List<EventsRouter.Outgoing> out = router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"" + LEAVER + "\",\"version\":1}");
        assertFalse(payload(out.get(0)).has("sagaId"),
                "no handle to echo means no field — never an invented one");
    }

    @Test
    void a_fact_with_a_mangled_handle_is_a_poison_pill() {
        // proceeding would purge the content under a verdict nobody can match: security's own
        // timeout then restores the account and apologises for content that is already gone
        assertEquals(List.of(), router.handle(FACTS, "{\"id\":\"" + UUID.randomUUID() + "\","
                + "\"sagaId\":\"not-a-uuid\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                + "\"email\":\"" + LEAVER + "\",\"version\":1}"));
        assertTrue(store.all().isEmpty(), "and no saga may open");
    }

    /** One sweeper pass as {@link KafkaLoop} makes it: a deadline later, deliveries reported. */
    private List<EventsRouter.Outgoing> sweep() {
        now = now.plus(TIMEOUT).plusSeconds(1);
        List<EventsRouter.Outgoing> out = router.sweepOverdue();
        out.stream()
                .map(EventsRouter.Outgoing::countsRetryFor)
                .filter(java.util.Objects::nonNull)
                .forEach(saga -> store.retryDelivered(saga, now));
        return out;
    }

    private static EventsRouter.Outgoing onOutcomes(List<EventsRouter.Outgoing> out) {
        return out.stream()
                .filter(outgoing -> EventsRouter.OUTCOMES_TOPIC.equals(outgoing.topic()))
                .findFirst().orElseThrow();
    }

    private static String fact(UUID factId, UUID securitySagaId) {
        return "{\"id\":\"" + factId + "\",\"sagaId\":\"" + securitySagaId + "\","
                + "\"type\":\"ACCOUNT_DELETION_REQUESTED\",\"email\":\"" + LEAVER
                + "\",\"version\":1}";
    }

    private JsonNode payload(EventsRouter.Outgoing outgoing) throws Exception {
        return mapper.readTree(outgoing.payload());
    }
}
