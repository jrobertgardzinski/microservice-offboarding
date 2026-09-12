package com.jrobertgardzinski.offboarding.boundary;

import com.jrobertgardzinski.offboarding.control.EventsRouter;
import com.jrobertgardzinski.offboarding.control.Observations;
import com.jrobertgardzinski.offboarding.entity.Observation;
import com.jrobertgardzinski.offboarding.control.RouterFixture;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The boundary, asserted instead of promised: observability is a layer this service can be
 * assembled WITHOUT.
 *
 * <p>The second half is unusually cheap to prove here, and deliberately so: every other test in
 * this module drives the router and the loop through the constructors that default to
 * {@code Observations.SILENT}. The whole saga suite — the pacts, the sweeper, the outbox, the
 * poison pills — already runs unwatched and passes, which is exactly the claim. What is left for
 * this test is to say so out loud, and to guard the direction of the dependency.
 */
@Epic("Architecture")
@Feature("Observability is a layer, not a dependency")
class ObservabilityIsOptionalTest {

    /**
     * The layers that must not name a tool. They are sibling MODULES now, which is what makes this
     * guard cheap to trust: a leak would have to be an import across a Maven dependency that does
     * not exist, not merely a line in the wrong package.
     */
    private static final List<Path> ABOVE_BOUNDARY = List.of(
            Path.of("../offboarding-entity/src/main/java"),
            Path.of("../offboarding-control/src/main/java"));

    /**
     * Vendor words, not concepts. {@code observ} is deliberately absent: {@code Observation} and
     * {@code Observations} ARE this service's own vocabulary and must be free to appear.
     */
    private static final List<String> TOOLS = List.of(
            "micrometer", "prometheus", "meterregistry", "opentelemetry", "otel",
            "traceparent", "grafana", "loki", "tempo", "_total", "metricsendpoint");

    @Test
    @DisplayName("nothing outside the boundary names the tool that watches it")
    void the_tool_stays_in_the_adapter() throws IOException {
        for (Path layer : ABOVE_BOUNDARY) {
            try (Stream<Path> tree = Files.walk(layer)) {
                List<String> leaks = tree
                        .filter(file -> file.toString().endsWith(".java"))
                        .filter(file -> mentionsATool(read(file)))
                        .map(Path::toString)
                        .toList();
                assertEquals(List.of(), leaks,
                        "these files outside the boundary name a watching tool — that is the"
                                + " coupling this layering exists to prevent: " + leaks);
            }
        }
    }

    @Test
    @DisplayName("the saga can be assembled with nobody listening, and is — by every other test here")
    void the_router_runs_unwatched() {
        RouterFixture unwatched = RouterFixture.router();

        // the fixture uses the constructor without a watcher, and the whole case still travels:
        // a deletion fact in, a purge command out. An unobserved orchestrator is an orchestrator
        // nobody is watching, not a broken one
        List<EventsRouter.Outgoing> out = unwatched.router.handle(RouterFixture.FACTS_TOPIC,
                "{\"id\":\"" + java.util.UUID.randomUUID() + "\","
                        + "\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");

        assertEquals(1, out.size());
        assertEquals(EventsRouter.COMMANDS_TOPIC, out.getFirst().topic());
        assertTrue(out.getFirst().payload().contains("PURGE_USER_CONTENT"), out.getFirst().payload());
    }

    private static boolean mentionsATool(String source) {
        String lower = source.toLowerCase(Locale.ROOT);
        return TOOLS.stream().anyMatch(lower::contains);
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException unreadable) {
            throw new IllegalStateException("cannot read " + file, unreadable);
        }
    }
}
