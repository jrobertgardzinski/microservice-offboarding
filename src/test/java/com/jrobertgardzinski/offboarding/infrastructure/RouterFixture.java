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

    private RouterFixture() {
        Set<String> participants = Set.copyOf(PARTICIPANT_BY_TOPIC.values());
        router = new EventsRouter(FACTS_TOPIC, PARTICIPANT_BY_TOPIC,
                new BeginOffboarding(store, participants),
                new RecordConfirmation(store, participants),
                new SweepOverdue(store, Duration.ofMinutes(2)),
                new ObjectMapper(),
                Clock.fixed(Instant.parse("2026-07-11T12:00:00Z"), ZoneOffset.UTC));
    }

    static RouterFixture router() {
        return new RouterFixture();
    }

    /** Seed a running saga so a confirmation has something to confirm. */
    RouterFixture withRunningSagaFor(String email) {
        store.start(UUID.randomUUID(), email, Instant.parse("2026-07-11T11:59:00Z"));
        return this;
    }
}
