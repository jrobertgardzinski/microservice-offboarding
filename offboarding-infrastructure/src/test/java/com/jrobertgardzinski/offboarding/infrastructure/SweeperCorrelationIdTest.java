package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.Destination;
import com.jrobertgardzinski.offboarding.application.EventsRouter;
import com.jrobertgardzinski.offboarding.domain.Opening;
import com.jrobertgardzinski.offboarding.system.BeginOffboarding;
import com.jrobertgardzinski.offboarding.domain.FakeSagaStore;
import com.jrobertgardzinski.offboarding.system.RecordConfirmation;
import com.jrobertgardzinski.offboarding.system.SweepOverdue;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Everything the sweeper publishes carries a correlation id — including the events that announce
 * nothing and charge no retry. The re-command has its own guard against a real broker
 * ({@code KafkaLoopIntegrationTest}); this is about the ones that used to travel bare, on exactly
 * the failure paths an operator follows an id through.
 */
@Epic("Infrastructure")
@Feature("Correlation id")
class SweeperCorrelationIdTest {

    private static final String LEAVER = "alice@example.com";
    private static final Instant T0 = Instant.parse("2026-07-11T12:00:00Z");

    @Test
    void the_compensation_command_carries_the_same_saga_id_as_the_verdict_it_rides_with() {
        // THE finding this pins. sweepCid read announcesSaga and countsRetryFor — both null for
        // every participant COMMAND the sweeper sends. So the PORTAL_PURGE_FAILED went out with
        // saga-xxxxxxxx while the RESTORE_USER_CONTENT that is supposed to give the leaver's
        // content back went out with no trace at all: an operator following the verdict's id sees
        // the participants' restore logs and nothing linking the command that caused them — and if
        // the restore is what went missing, the one hop that would prove it was sent has no id
        FakeSagaStore store = new FakeSagaStore();
        UUID saga = store.start(new Opening(UUID.randomUUID(), LEAVER, null, null,
                Set.of("memes")), T0);
        List<EventsRouter.Outgoing> swept = capitulatingSweepOver(store);

        EventsRouter.Outgoing restore = only(swept, Destination.PARTICIPANTS);
        EventsRouter.Outgoing verdict = only(swept, Destination.SECURITY);
        String expected = "saga-" + saga.toString().substring(0, 8);

        assertNotNull(KafkaLoop.sweepCid(restore),
                "the compensation command must carry a correlation id — it is half of the story"
                        + " the failure verdict tells");
        assertEquals(expected, KafkaLoop.sweepCid(restore),
                "and it must be the id of the saga it belongs to");
        assertEquals(KafkaLoop.sweepCid(verdict), KafkaLoop.sweepCid(restore),
                "the two went out in one breath and must be followable as one conversation");
    }

    /** One sweeper pass over a saga nobody ever confirmed, with the retries already spent. */
    private static List<EventsRouter.Outgoing> capitulatingSweepOver(FakeSagaStore store) {
        Set<String> participants = Set.of("memes");
        EventsRouter router = new EventsRouter(
                new BeginOffboarding(store, participants),
                new RecordConfirmation(store, participants),
                new SweepOverdue(store, Duration.ofSeconds(120), 0, SweepOverdue.DEFAULT_REPUBLISH_AFTER,
                        SweepOverdue.DEFAULT_RETENTION),
                new ObjectMapper(), Clock.fixed(T0.plusSeconds(600), ZoneOffset.UTC));
        return router.sweepOverdue();
    }

    private static EventsRouter.Outgoing only(List<EventsRouter.Outgoing> swept,
                                              Destination destination) {
        return swept.stream().filter(outgoing -> outgoing.destination() == destination)
                .reduce((first, second) -> {
                    throw new AssertionError("expected one event for " + destination);
                }).orElseThrow();
    }
}
