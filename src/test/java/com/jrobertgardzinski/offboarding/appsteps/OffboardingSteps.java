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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

    @Given("security announced that {word} requested deletion choosing memes={word} and comments={word}")
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

    @When("security announces another deletion request for {word}")
    public void anotherDeletionRequested(String email) {
        // a genuinely new fact — a fresh id, not a replay of the first announcement
        announced.addAll(router.handle(FACTS_TOPIC,
                "{\"id\":\"" + java.util.UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"" + email + "\",\"version\":1}"));
    }

    @When("security replays the deletion fact for {word}")
    public void deletionFactReplayed(String email) {
        deletionRequested(email);   // byte-for-byte the fact that opened the case
    }

    @When("security announces a deletion request that names no account")
    public void deletionRequestNamingNoAccount() {
        announced.addAll(router.handle(FACTS_TOPIC,
                "{\"id\":\"" + java.util.UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"version\":1}"));
    }

    @When("security announces a deletion request with a garbled identity")
    public void deletionRequestWithGarbledIdentity() {
        announced.addAll(router.handle(FACTS_TOPIC,
                "{\"id\":\"not-a-uuid\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"mallory@example.com\",\"version\":1}"));
    }

    @Given("{word} confirmed its purge for {word}")
    @When("{word} confirms its purge for {word}")
    public void participantConfirms(String participant, String email) {
        announced.addAll(router.handle(topicOf(participant),
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"" + email + "\",\"version\":1}"));
    }

    @When("{word} confirms its purge for {word} echoing the purge command")
    public void participantConfirmsEchoingTheCommand(String participant, String email) {
        confirmEchoing(participant, email, commandedSagaId());
    }

    @When("every content service confirms its purge for {word} echoing the purge command the portal gave up on")
    public void everyParticipantConfirmsTheAbandonedCommand(String email) {
        // the FIRST command is the one the portal later gave up on — a newer case for the same
        // account may have commanded again since, and that one was not given up
        String sagaId = firstCommandedSagaId();
        for (String participant : List.of("memes", "comments", "collections")) {
            confirmEchoing(participant, email, sagaId);
        }
    }

    private void confirmEchoing(String participant, String email, String sagaId) {
        announced.addAll(router.handle(topicOf(participant),
                "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"" + email + "\","
                        + "\"sagaId\":\"" + sagaId + "\",\"version\":1}"));
    }

    private static String topicOf(String participant) {
        return switch (participant) {
            case "memes" -> "memes-events";
            case "comments" -> "comments-events";
            case "collections" -> "usercollections-events";
            default -> throw new IllegalArgumentException("unknown participant " + participant);
        };
    }

    /** The saga the (latest) purge command carried — what a fresh confirmation would echo. */
    private String commandedSagaId() {
        List<JsonNode> commands = allOn(EventsRouter.COMMANDS_TOPIC);
        assertFalse(commands.isEmpty(), "no purge command went out to echo");
        return commands.get(commands.size() - 1).path("sagaId").asText();
    }

    /** The saga the FIRST purge command carried — what a late echo of an old case would carry. */
    private String firstCommandedSagaId() {
        List<JsonNode> commands = allOn(EventsRouter.COMMANDS_TOPIC);
        assertFalse(commands.isEmpty(), "no purge command went out to echo");
        return commands.get(0).path("sagaId").asText();
    }

    @Given("the announcement reached security")
    public void announcementReachedSecurity() {
        List<EventsRouter.Outgoing> outcomes = announced.stream()
                .filter(o -> o.topic().equals(EventsRouter.OUTCOMES_TOPIC)).toList();
        assertFalse(outcomes.isEmpty(), "there is no announcement to have reached security");
        outcomes.forEach(outcome -> store.markAnnounced(outcome.announcesSaga()));
        announced.removeAll(outcomes);   // delivered and consumed; the story moves on
    }

    @Given("the announcement never left the portal")
    public void announcementNeverLeftThePortal() {
        // lost in transit before anyone could note it as announced — the saga keeps owing it
        announced.removeIf(o -> o.topic().equals(EventsRouter.OUTCOMES_TOPIC));
    }

    @When("the purge deadline passes")
    public void deadlinePasses() {
        now = now.plus(TIMEOUT).plusSeconds(1);
        announced.addAll(sweepWithDeliveredRetries());
    }

    @Given("the purge deadline passed and every retry was exhausted")
    @When("the purge deadline passes and every retry is exhausted")
    public void deadlinePassesAndRetriesRunOut() {
        now = now.plus(TIMEOUT).plusSeconds(1);
        // one sweep per retry, plus the sweep that finally capitulates
        for (int sweep = 0; sweep <= SweepOverdue.DEFAULT_MAX_RETRIES; sweep++) {
            announced.addAll(sweepWithDeliveredRetries());
        }
    }

    @When("the next sweep comes around")
    public void nextSweepComesAround() {
        // late enough that a still-owed outcome is no longer "merely in flight"
        now = now.plus(SweepOverdue.DEFAULT_REPUBLISH_AFTER).plusSeconds(1);
        announced.addAll(sweepWithDeliveredRetries());
    }

    @When("the retention period passes")
    public void retentionPeriodPasses() {
        now = now.plus(SweepOverdue.DEFAULT_RETENTION).plusSeconds(1);
        announced.addAll(sweepWithDeliveredRetries());
    }

    /**
     * These scenarios assume the transport DELIVERS what the router answers — so, exactly like
     * {@code KafkaLoop.settleDeliveries} after a proven send, a re-commanded purge reports back
     * and burns one retry. (A broker outage, where nothing is delivered and nothing burns, is
     * the integration test's story, not the choreography's.)
     */
    private List<EventsRouter.Outgoing> sweepWithDeliveredRetries() {
        List<EventsRouter.Outgoing> swept = router.sweepOverdue();
        swept.stream()
                .map(EventsRouter.Outgoing::countsRetryFor)
                .filter(java.util.Objects::nonNull)
                .forEach(store::retryDelivered);
        return swept;
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

    @Then("every purge command carries the choices memes={word} and comments={word}")
    public void everyPurgeCommandCarriesPolicy(String memesRule, String commentsRule) {
        // the retry must repeat the ORIGINAL command — the choices stored with the saga, not the
        // participants' defaults; every command on the wire carries them identically
        List<JsonNode> commands = allOn(EventsRouter.COMMANDS_TOPIC);
        assertFalse(commands.isEmpty(), "no purge command went out to carry the choices");
        for (JsonNode command : commands) {
            JsonNode policy = command.path("policy");
            assertEquals(memesRule, policy.path("memes").asText(),
                    "the choices must survive into: " + command);
            assertEquals(commentsRule, policy.path("comments").asText(),
                    "the choices must survive into: " + command);
        }
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

    @Then("the purge command for {word} is sent again")
    public void purgeCommandResent(String email) {
        List<JsonNode> commands = allOn(EventsRouter.COMMANDS_TOPIC);
        assertEquals(2, commands.size(), "the original command and exactly one re-send");
        for (JsonNode command : commands) {
            assertEquals("PURGE_USER_CONTENT", command.path("type").asText());
            assertEquals(email, command.path("email").asText());
        }
        assertEquals(commands.get(0).path("sagaId").asText(), commands.get(1).path("sagaId").asText(),
                "the re-send commands the SAME saga, not a fork");
    }

    @Then("a fresh purge command for {word} opens a brand-new case")
    public void freshPurgeCommandOpensANewCase(String email) {
        List<JsonNode> commands = allOn(EventsRouter.COMMANDS_TOPIC);
        assertEquals(2, commands.size(), "the command of the finished case and the fresh one");
        for (JsonNode command : commands) {
            assertEquals("PURGE_USER_CONTENT", command.path("type").asText());
            assertEquals(email, command.path("email").asText());
        }
        assertNotEquals(commands.get(0).path("sagaId").asText(), commands.get(1).path("sagaId").asText(),
                "past retention the portal remembers nothing: the replayed fact opens a NEW case");
    }

    @Then("the portal never announces the content of {word} purged")
    public void portalPurgedNeverAnnounced(String email) {
        for (JsonNode outcome : allOn(EventsRouter.OUTCOMES_TOPIC)) {
            assertFalse("PORTAL_CONTENT_PURGED".equals(outcome.path("type").asText())
                            && email.equals(outcome.path("email").asText()),
                    "a late confirmation may not rewrite the announced outcome: " + outcome);
        }
    }

    @Then("the failure names {word} among the participants that already purged")
    public void failureNamesThePartialPurge(String participant) {
        JsonNode confirmed = onlyOn(EventsRouter.OUTCOMES_TOPIC).path("confirmed");
        boolean named = false;
        for (JsonNode name : confirmed) {
            named |= participant.equals(name.asText());
        }
        assertTrue(named, "the failure must disclose the partial purge: " + confirmed);
    }

    @Then("no outcome is announced yet")
    @Then("no outcome is announced again")
    public void nothingAnnounced() {
        List<EventsRouter.Outgoing> outcomes = announced.stream()
                .filter(o -> o.topic().equals(EventsRouter.OUTCOMES_TOPIC)).toList();
        assertEquals(List.of(), outcomes, "no outcome may be announced yet");
    }

    private JsonNode onlyOn(String topic) {
        List<JsonNode> matching = allOn(topic);
        assertEquals(1, matching.size(), "expected exactly one event on " + topic + ", got " + announced);
        return matching.get(0);
    }

    private List<JsonNode> allOn(String topic) {
        return announced.stream()
                .filter(o -> o.topic().equals(topic))
                .map(o -> {
                    try {
                        return mapper.readTree(o.payload());
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .toList();
    }
}
