package com.jrobertgardzinski.offboarding.appsteps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import com.jrobertgardzinski.offboarding.infrastructure.EventsRouter;
import com.jrobertgardzinski.offboarding.infrastructure.InMemorySagaStore;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scenarios drive the REAL router — the same code the Kafka loop calls — with the in-memory
 * store and a hand-wound clock; what the router answers is what the portal would publish.
 */
public class OffboardingSteps {

    private static final String FACTS_TOPIC = "security-events";
    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    private final ObjectMapper mapper = new ObjectMapper();
    private InMemorySagaStore store;
    private EventsRouter router;
    private Instant now;
    private final List<EventsRouter.Outgoing> announced = new ArrayList<>();

    @Before
    public void wire() {
        now = Instant.parse("2026-07-11T12:00:00Z");
        store = new InMemorySagaStore();
        withParticipants(Map.of(
                "memes-events", "memes",
                "comments-events", "comments",
                "usercollections-events", "collections"));
    }

    private void withParticipants(Map<String, String> participantByTopic) {
        Set<String> participants = Set.copyOf(participantByTopic.values());
        router = new EventsRouter(FACTS_TOPIC, participantByTopic,
                new BeginOffboarding(store, participants),
                new RecordConfirmation(store, participants),
                new SweepOverdue(store, TIMEOUT),
                mapper, tickingClock());
    }

    /** A clock the steps advance by reassigning {@link #now}. */
    private Clock tickingClock() {
        return new Clock() {
            @Override
            public Instant instant() {
                return now;
            }

            @Override
            public ZoneOffset getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }
        };
    }

    @Given("the portal has no content participants configured")
    public void noParticipants() {
        withParticipants(Map.of());
    }

    @Given("security announced that {word} requested deletion")
    @When("security announces that {word} requested deletion")
    public void deletionRequested(String email) {
        announced.addAll(router.handle(FACTS_TOPIC,
                "{\"id\":\"" + factId(email) + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"" + email + "\",\"version\":1}"));
    }

    @When("security announces that {word} requested deletion choosing memes={word} and comments={word}")
    public void deletionRequestedWithPolicy(String email, String memesRule, String commentsRule) {
        announced.addAll(router.handle(FACTS_TOPIC,
                "{\"id\":\"" + factId(email) + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"" + email + "\","
                        + "\"policy\":{\"memes\":\"" + memesRule + "\",\"comments\":\"" + commentsRule + "\"},"
                        + "\"version\":1}"));
    }

    private static java.util.UUID factId(String email) {
        return java.util.UUID.nameUUIDFromBytes(("fact:" + email).getBytes());
    }

    @Given("{word} confirmed its purge for {word}")
    @When("{word} confirms its purge for {word}")
    public void participantConfirms(String participant, String email) {
        String topic = switch (participant) {
            case "memes" -> "memes-events";
            case "comments" -> "comments-events";
            case "collections" -> "usercollections-events";
            default -> throw new IllegalArgumentException("unknown participant " + participant);
        };
        announced.addAll(router.handle(topic,
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"" + email + "\",\"version\":1}"));
    }

    @When("the purge deadline passes")
    public void deadlinePasses() {
        now = now.plus(TIMEOUT).plusSeconds(1);
        announced.addAll(router.sweepOverdue());
    }

    @Then("a purge command for {word} goes out to the content services")
    public void purgeCommandWentOut(String email) {
        JsonNode command = onlyOn(EventsRouter.COMMANDS_TOPIC);
        assertEquals("PURGE_USER_CONTENT", command.path("type").asText());
        assertEquals(email, command.path("email").asText());
        assertTrue(command.hasNonNull("sagaId"), "participants confirm by saga");
    }

    @Then("the purge command carries the choices memes={word} and comments={word}")
    public void purgeCommandCarriesPolicy(String memesRule, String commentsRule) {
        JsonNode policy = onlyOn(EventsRouter.COMMANDS_TOPIC).path("policy");
        assertEquals(memesRule, policy.path("memes").asText());
        assertEquals(commentsRule, policy.path("comments").asText());
    }

    @Then("the portal announces the content of {word} purged")
    public void portalPurgedAnnounced(String email) {
        JsonNode outcome = onlyOn(EventsRouter.OUTCOMES_TOPIC);
        assertEquals("PORTAL_CONTENT_PURGED", outcome.path("type").asText());
        assertEquals(email, outcome.path("email").asText());
    }

    @Then("the portal announces the purge for {word} failed")
    public void purgeFailureAnnounced(String email) {
        JsonNode outcome = onlyOn(EventsRouter.OUTCOMES_TOPIC);
        assertEquals("PORTAL_PURGE_FAILED", outcome.path("type").asText());
        assertEquals(email, outcome.path("email").asText());
    }

    @Then("no outcome is announced yet")
    public void nothingAnnounced() {
        List<EventsRouter.Outgoing> outcomes = announced.stream()
                .filter(o -> o.topic().equals(EventsRouter.OUTCOMES_TOPIC)).toList();
        assertEquals(List.of(), outcomes, "no outcome may be announced yet");
    }

    private JsonNode onlyOn(String topic) {
        List<EventsRouter.Outgoing> matching = announced.stream()
                .filter(o -> o.topic().equals(topic)).toList();
        assertEquals(1, matching.size(), "expected exactly one event on " + topic + ", got " + announced);
        try {
            return mapper.readTree(matching.get(0).payload());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
