package com.jrobertgardzinski.offboarding.infrastructure;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The mini-outbox, end to end at the router level: finishing a saga does not announce it — the
 * announced mark is earned only when the loop's flush succeeds — so an outcome that never got the
 * mark is re-published by the sweep, and one that did is not. This is what makes the outcome
 * survive a crash between the state flip and the producer flush (the redelivery-after-COMPLETED
 * guarantee).
 */
class OutcomeOutboxTest {

    private static final String FACTS = "security-events";
    private static final Map<String, String> TOPICS = Map.of("memes-events", "memes");

    private final InMemorySagaStore store = new InMemorySagaStore();
    private final EventsRouter router;
    private Instant now = Instant.parse("2026-07-11T12:00:00Z");

    OutcomeOutboxTest() {
        Set<String> participants = Set.copyOf(TOPICS.values());
        router = new EventsRouter(FACTS, TOPICS,
                new BeginOffboarding(store, participants),
                new RecordConfirmation(store, participants),
                new SweepOverdue(store, Duration.ofMinutes(2)),
                new ObjectMapper(),
                new Clock() {
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
                });
    }

    private UUID completeASaga() {
        router.handle(FACTS, "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                + "\"email\":\"leaver@example.com\",\"version\":1}");
        List<EventsRouter.Outgoing> outcome = router.handle("memes-events",
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"leaver@example.com\",\"version\":1}");
        // two events, in this order: the CLOSURE that lets the participants erase for real, then
        // the single verdict security waits for. The mark was reversible up to this instant
        assertEquals(2, outcome.size(), "the completing confirmation closes the saga and announces it");
        assertEquals(EventsRouter.COMMANDS_TOPIC, outcome.get(0).topic());
        assertTrue(outcome.get(0).payload().contains("\"" + EventsRouter.ERASE_COMMAND + "\""));
        assertEquals(EventsRouter.OUTCOMES_TOPIC, outcome.get(1).topic());
        return outcome.get(1).announcesSaga();
    }

    @Test
    void an_unannounced_completion_is_republished_by_the_sweep() {
        UUID sagaId = completeASaga();
        // the crash: the outcome above never reached the broker, so nobody marked it announced
        now = now.plus(SweepOverdue.DEFAULT_REPUBLISH_AFTER).plusSeconds(1);
        List<EventsRouter.Outgoing> swept = router.sweepOverdue();
        // the PAIR is re-published, not just the verdict: the two went out together and are
        // withheld together, so an unannounced outcome means the closure may be missing too — and
        // a closure nobody repeats leaves the leaver's content hidden but never erased
        assertEquals(2, swept.size(), "the sweep owes the world this closure AND this outcome");
        assertEquals(EventsRouter.COMMANDS_TOPIC, swept.get(0).topic());
        assertTrue(swept.get(0).payload().contains("\"" + EventsRouter.ERASE_COMMAND + "\""));
        assertEquals(sagaId, swept.get(0).partOfSaga(),
                "the re-commanded closure shares the outcome's fate at the outbox");
        assertEquals(EventsRouter.OUTCOMES_TOPIC, swept.get(1).topic());
        assertTrue(swept.get(1).payload().contains("\"PORTAL_CONTENT_PURGED\""));
        assertEquals(sagaId, swept.get(1).announcesSaga(),
                "the re-published outcome names its saga so the loop can settle the mark");
    }

    @Test
    void a_marked_outcome_is_never_republished() {
        UUID sagaId = completeASaga();
        store.markAnnounced(sagaId);   // what the loop does right after a successful flush
        now = now.plus(SweepOverdue.DEFAULT_REPUBLISH_AFTER).plusSeconds(1);
        assertEquals(List.of(), router.sweepOverdue());
    }

    @Test
    void a_fresh_unannounced_outcome_waits_out_the_age_guard() {
        completeASaga();
        now = now.plus(SweepOverdue.DEFAULT_REPUBLISH_AFTER).minusSeconds(1);
        assertEquals(List.of(), router.sweepOverdue(),
                "an outcome merely in flight must not double just because a sweep ran");
    }

    @Test
    void the_instantly_clean_outcome_rides_the_same_outbox() {
        EventsRouter bare = new EventsRouter(FACTS, Map.of(),
                new BeginOffboarding(store, Set.of()),
                new RecordConfirmation(store, Set.of()),
                new SweepOverdue(store, Duration.ofMinutes(2)),
                new ObjectMapper(), Clock.fixed(now, ZoneOffset.UTC));
        List<EventsRouter.Outgoing> out = bare.handle(FACTS,
                "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");
        assertEquals(store.all().get(0).id, out.get(0).announcesSaga(),
                "even the no-participants shortcut owes its announced mark to the outbox");
    }
}
