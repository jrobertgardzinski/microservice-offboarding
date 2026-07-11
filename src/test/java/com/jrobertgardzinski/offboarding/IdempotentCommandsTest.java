package com.jrobertgardzinski.offboarding;

import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import com.jrobertgardzinski.offboarding.infrastructure.InMemorySagaStore;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The teeth of workspace ADR 0006: every command is idempotent BY DEFAULT — running it twice
 * must leave exactly the state running it once leaves. One generic test enforces the law for the
 * whole service; the feature file's scenarios pin the message contracts instead. (What a command
 * ANSWERS may differ between runs — the completion is announced exactly once by design — but the
 * saga STATE may not.) A new command joins the law by joining COMMANDS below.
 */
class IdempotentCommandsTest {

    private static final Set<String> PARTICIPANTS = Set.of("memes", "comments", "collections");
    private static final Instant T0 = Instant.parse("2026-07-11T12:00:00Z");
    private static final java.util.UUID ALICE_FACT =
            java.util.UUID.fromString("00000000-0000-0000-0000-00000000a11c");
    private static final java.util.UUID CAROL_FACT =
            java.util.UUID.fromString("00000000-0000-0000-0000-0000000ca201");

    private static final Map<String, Consumer<InMemorySagaStore>> COMMANDS = commands();

    private static Map<String, Consumer<InMemorySagaStore>> commands() {
        Map<String, Consumer<InMemorySagaStore>> c = new LinkedHashMap<>();
        c.put("begin a fresh offboarding",
                store -> new BeginOffboarding(store, PARTICIPANTS).execute(CAROL_FACT, "carol@example.com", T0));
        c.put("begin over an already-running saga",
                store -> new BeginOffboarding(store, PARTICIPANTS).execute(ALICE_FACT, "alice@example.com", T0));
        c.put("begin with no participants (instant completion)",
                store -> new BeginOffboarding(store, Set.of()).execute(CAROL_FACT, "carol@example.com", T0));
        c.put("record a first confirmation",
                store -> new RecordConfirmation(store, PARTICIPANTS).execute("alice@example.com", "comments", T0));
        c.put("record an already-recorded confirmation",
                store -> new RecordConfirmation(store, PARTICIPANTS).execute("alice@example.com", "memes", T0));
        c.put("record the completing confirmation",
                store -> {
                    RecordConfirmation confirm = new RecordConfirmation(store, PARTICIPANTS);
                    confirm.execute("alice@example.com", "comments", T0);
                    confirm.execute("alice@example.com", "collections", T0);
                });
        c.put("record a stray confirmation (no saga)",
                store -> new RecordConfirmation(store, PARTICIPANTS).execute("nobody@example.com", "memes", T0));
        c.put("sweep the overdue",
                store -> new SweepOverdue(store, Duration.ofMinutes(2))
                        .execute(T0.plus(Duration.ofMinutes(10))));
        return c;
    }

    /** A saga already running for alice, with memes confirmed. */
    private static InMemorySagaStore seeded() {
        InMemorySagaStore store = new InMemorySagaStore();
        new BeginOffboarding(store, PARTICIPANTS).execute(ALICE_FACT, "alice@example.com", T0);
        new RecordConfirmation(store, PARTICIPANTS).execute("alice@example.com", "memes", T0);
        return store;
    }

    /** The observable state, flattened — what "the same state" means in the law. */
    private static List<String> fingerprint(InMemorySagaStore store) {
        return store.all().stream()
                .map(saga -> saga.email + "|" + saga.state + "|" + new TreeSet<>(saga.confirmed))
                .sorted()
                .toList();
    }

    @TestFactory
    Stream<DynamicTest> every_command_twice_equals_once() {
        return COMMANDS.entrySet().stream().map(entry -> DynamicTest.dynamicTest(
                entry.getKey(), () -> {
                    InMemorySagaStore once = seeded();
                    entry.getValue().accept(once);

                    InMemorySagaStore twice = seeded();
                    entry.getValue().accept(twice);
                    entry.getValue().accept(twice);

                    assertEquals(fingerprint(once), fingerprint(twice),
                            "ADR 0006: a command run twice must leave the state of one run");
                }));
    }
}
