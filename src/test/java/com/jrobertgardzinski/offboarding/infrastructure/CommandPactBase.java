package com.jrobertgardzinski.offboarding.infrastructure;

import java.util.List;
import java.util.UUID;

/**
 * The REAL command-producing paths, shared by the provider tests: a deletion fact (and then a
 * participant's confirmation, or a sweep that gives up) goes through the router, and what it
 * answers with is the payload the participants' pacts are verified against — never hand-written
 * JSON. All three of the saga's commands are produced here by driving the router to the point
 * where it genuinely sends them.
 */
public final class CommandPactBase {

    private CommandPactBase() {
    }

    public static String realPurgeCommand() {
        return RouterFixture.router().router.handle(RouterFixture.FACTS_TOPIC,
                        "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                                + "\"email\":\"leaver@example.com\",\"version\":1}")
                .get(0).payload();
    }

    public static String realPurgeCommandWithPolicy() {
        return RouterFixture.router().router.handle(RouterFixture.FACTS_TOPIC,
                        "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                                + "\"email\":\"leaver@example.com\","
                                + "\"policy\":{\"memes\":\"KEEP_POPULAR_ANONYMIZED:5\",\"comments\":\"DELETE\"},"
                                + "\"version\":1}")
                .get(0).payload();
    }

    /**
     * The CLOSURE, produced the only way the router ever produces one: every required participant
     * confirms, and the last confirmation emits it. The policy the fact carried is ferried back
     * out on it — the participants apply their rule at erasure time, which is why the closure and
     * not the mark is what needs it.
     */
    public static String realEraseCommand() {
        RouterFixture fixture = RouterFixture.router();
        fixture.router.handle(RouterFixture.FACTS_TOPIC,
                "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"leaver@example.com\","
                        + "\"policy\":{\"memes\":\"DELETE\",\"comments\":\"ANONYMIZE_AUTHOR\"},"
                        + "\"version\":1}");
        List<EventsRouter.Outgoing> out = List.of();
        for (String topic : RouterFixture.PARTICIPANT_BY_TOPIC.keySet()) {
            out = fixture.router.handle(topic, confirmation());
        }
        return onCommands(out);
    }

    /**
     * The COMPENSATION, produced the only way the router ever produces one: a saga nobody confirms
     * runs out of retries and the sweep gives up. Started an hour before the fixture's fixed
     * "now", so the very first sweep of the capitulating fixture is the one that compensates.
     */
    public static String realRestoreCommand() {
        RouterFixture fixture = RouterFixture.routerCapitulatingAtOnce();
        fixture.store.start(new com.jrobertgardzinski.offboarding.application.SagaStore.Opening(
                        UUID.randomUUID(), "leaver@example.com", null, UUID.randomUUID(),
                        java.util.Set.copyOf(RouterFixture.PARTICIPANT_BY_TOPIC.values())),
                java.time.Instant.parse("2026-07-11T11:00:00Z"));
        return onCommands(fixture.router.sweepOverdue());
    }

    private static String confirmation() {
        return "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"leaver@example.com\",\"version\":1}";
    }

    /** The one event on the participants' topic — the commands and the verdict travel together. */
    private static String onCommands(List<EventsRouter.Outgoing> out) {
        return out.stream()
                .filter(outgoing -> EventsRouter.COMMANDS_TOPIC.equals(outgoing.topic()))
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "the router produced no participant command: " + out))
                .payload();
    }
}
