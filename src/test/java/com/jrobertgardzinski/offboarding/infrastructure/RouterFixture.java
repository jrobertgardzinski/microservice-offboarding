package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** The default wiring the contract tests drive: the real router over the in-memory store. */
final class RouterFixture {

    static final String FACTS_TOPIC = "security-events";
    static final Map<String, String> PARTICIPANT_BY_TOPIC = Map.of(
            "memes-events", "memes",
            "comments-events", "comments",
            "usercollections-events", "collections");

    final InMemorySagaStore store = new InMemorySagaStore();
    final EventsRouter router;

    private RouterFixture(int maxRetries) {
        Set<String> participants = Set.copyOf(PARTICIPANT_BY_TOPIC.values());
        router = new EventsRouter(FACTS_TOPIC, PARTICIPANT_BY_TOPIC,
                new BeginOffboarding(store, participants),
                new RecordConfirmation(store, participants),
                new SweepOverdue(store, Duration.ofMinutes(2), maxRetries,
                        SweepOverdue.DEFAULT_REPUBLISH_AFTER, SweepOverdue.DEFAULT_RETENTION),
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-11T12:00:00Z"), ZoneOffset.UTC));
    }

    static RouterFixture router() {
        return new RouterFixture(SweepOverdue.DEFAULT_MAX_RETRIES);
    }

    /**
     * The same wiring with no retry budget, so ONE sweep of an overdue saga capitulates. The
     * fixture's clock is FIXED, and a delivered re-command now buys the participant a whole
     * timeout measured from that delivery — so with retries in play no amount of sweeping at one
     * instant can ever reach the capitulation. What this fixture is for: examples that need the
     * failure verdict itself, not the road to it (the road is pinned by JdbcSagaStoreTest and the
     * Gherkin scenarios, both of which can move their clocks).
     */
    static RouterFixture routerCapitulatingAtOnce() {
        return new RouterFixture(0);
    }

    /** Seed a running saga so a confirmation has something to confirm. */
    RouterFixture withRunningSagaFor(String email) {
        store.start(UUID.randomUUID(), email, Instant.parse("2026-07-11T11:59:00Z"));
        return this;
    }

    /**
     * Seed a running saga under a KNOWN id — for confirmations that echo it. An echoed id that
     * matches no RUNNING saga is a stray by design, so a pact example's fixed sagaId must be the
     * id the seeded saga actually has.
     */
    RouterFixture withRunningSaga(UUID sagaId, String email) {
        store.startWithId(sagaId, UUID.randomUUID(), email, Instant.parse("2026-07-11T11:59:00Z"));
        return this;
    }

    /** A running saga carrying security's handle — what the verdict has to echo. */
    RouterFixture withRunningSagaFor(String email, UUID securitySagaId) {
        store.startWithId(UUID.randomUUID(), UUID.randomUUID(), email, securitySagaId,
                Instant.parse("2026-07-11T11:59:00Z"));
        return this;
    }

    /** The sagaId a confirmation payload echoes — what {@link #withRunningSaga} should seed. */
    static UUID sagaIdOf(String confirmationPayload) throws Exception {
        return UUID.fromString(
                new ObjectMapper().readTree(confirmationPayload).path("sagaId").asText());
    }
}
