package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The saga's switchboard, pure and broker-free so the Gherkin scenarios and the pact tests drive
 * it directly. In: security's {@code ACCOUNT_DELETION_REQUESTED} fact and the participants'
 * {@code USER_CONTENT_PURGED} confirmations (participant = topic, exactly as before the
 * extraction). Out: the {@code PURGE_USER_CONTENT} command — byte-compatible with the one
 * security's orchestrator used to emit, so the participants never noticed the changing of the
 * guard — and the single outcome security waits for: {@code PORTAL_CONTENT_PURGED} or
 * {@code PORTAL_PURGE_FAILED}. The leaver's policy choices ride the command verbatim — their
 * vocabulary belongs to the content services, this one only ferries.
 */
public class EventsRouter {

    public static final String COMMANDS_TOPIC = "content-commands";
    public static final String OUTCOMES_TOPIC = "offboarding-events";

    /** An event to publish: the loop adds the correlation-id header and sends. */
    public record Outgoing(String topic, String key, String payload) {
    }

    private static final Logger LOG = LoggerFactory.getLogger(EventsRouter.class);

    private final String factsTopic;
    private final Map<String, String> participantByTopic;
    private final BeginOffboarding begin;
    private final RecordConfirmation confirm;
    private final SweepOverdue sweep;
    private final ObjectMapper mapper;
    private final Clock clock;

    public EventsRouter(String factsTopic, Map<String, String> participantByTopic,
                        BeginOffboarding begin, RecordConfirmation confirm, SweepOverdue sweep,
                        ObjectMapper mapper, Clock clock) {
        this.factsTopic = factsTopic;
        this.participantByTopic = participantByTopic;
        this.begin = begin;
        this.confirm = confirm;
        this.sweep = sweep;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** Route one consumed record to its use case; returns what to publish in response. */
    public List<Outgoing> handle(String topic, String payload) {
        JsonNode event;
        try {
            event = mapper.readTree(payload);
        } catch (Exception malformed) {
            LOG.warn("dropping malformed event on {}: {}", topic, payload);
            return List.of();
        }
        String type = event.path("type").asText();
        if (topic.equals(factsTopic) && "ACCOUNT_DELETION_REQUESTED".equals(type)) {
            return onDeletionRequested(event);
        }
        String participant = participantByTopic.get(topic);
        if (participant != null && "USER_CONTENT_PURGED".equals(type)) {
            return onConfirmation(event, participant);
        }
        return List.of();   // other lifecycle events share these topics; not ours
    }

    /** The timeout sweep: overdue sagas compensate and each failure is announced. */
    public List<Outgoing> sweepOverdue() {
        List<Outgoing> out = new ArrayList<>();
        for (String email : sweep.execute(Instant.now(clock))) {
            LOG.warn("portal purge overdue for {}; announcing the failure", email);
            out.add(outcome("PORTAL_PURGE_FAILED", email));
        }
        return out;
    }

    private List<Outgoing> onDeletionRequested(JsonNode fact) {
        String email = fact.path("email").asText();
        // the fact's id is the replay key: the same fact twice finds the same saga. A fact
        // without one gets a random id — honest about having no replay protection to offer.
        UUID factId = fact.hasNonNull("id")
                ? UUID.fromString(fact.get("id").asText())
                : UUID.randomUUID();
        BeginOffboarding.Begun begun = begin.execute(factId, email, Instant.now(clock));
        if (begun.completedImmediately()) {
            LOG.info("no content participants configured; portal instantly clean for {}", email);
            return List.of(outcome("PORTAL_CONTENT_PURGED", email));
        }
        ObjectNode command = mapper.createObjectNode()
                .put("id", UUID.randomUUID().toString())
                .put("sagaId", begun.sagaId().toString())
                .put("type", "PURGE_USER_CONTENT")
                .put("email", email)
                // envelope version (workspace ADR 0004): fields only ever added within version 1
                .put("version", 1);
        JsonNode policy = fact.path("policy");
        if (policy.isObject()) {
            command.set("policy", policy);   // the leaver's choices, ferried untouched
        }
        LOG.info("commanding the content purge for {} (saga {})", email, begun.sagaId());
        return List.of(new Outgoing(COMMANDS_TOPIC, email, write(command)));
    }

    private List<Outgoing> onConfirmation(JsonNode confirmation, String participant) {
        String email = confirmation.path("email").asText();
        if (!confirm.execute(email, participant, Instant.now(clock))) {
            LOG.info("recorded {} purge confirmation for {}; saga not complete yet", participant, email);
            return List.of();
        }
        LOG.info("all participants confirmed for {}; announcing the portal purged", email);
        return List.of(outcome("PORTAL_CONTENT_PURGED", email));
    }

    private Outgoing outcome(String type, String email) {
        return new Outgoing(OUTCOMES_TOPIC, email, write(mapper.createObjectNode()
                .put("id", UUID.randomUUID().toString())
                .put("type", type)
                .put("email", email)
                .put("version", 1)));
    }

    private String write(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (Exception impossible) {
            throw new IllegalStateException("could not serialise event", impossible);
        }
    }
}
