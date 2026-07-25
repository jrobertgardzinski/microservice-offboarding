package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SagaStore;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
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
 *
 * <p>Records that cannot possibly be routed — no parsable id on a fact, a present-but-unparseable
 * sagaId on a confirmation, no email anywhere — are poison pills: logged and dropped, exactly
 * like malformed JSON, so one bad record can never wedge the topic behind it. Outcomes carry the saga they announce ({@link Outgoing#announcesSaga})
 * for the loop's outbox mark; the sweep re-emits whatever never got that mark.
 */
public class EventsRouter {

    public static final String COMMANDS_TOPIC = "content-commands";
    public static final String OUTCOMES_TOPIC = "offboarding-events";

    /**
     * An event to publish: the loop adds the correlation-id header and sends. When the event is a
     * saga's outcome, {@code announcesSaga} names it so the loop can mark the outbox after a
     * successful flush; when it is the sweeper RE-commanding an overdue purge,
     * {@code countsRetryFor} names the saga whose retry counter the loop charges once the send is
     * proven delivered ({@link com.jrobertgardzinski.offboarding.application.SagaStore#retryDelivered}).
     * Everything else leaves both null.
     */
    public record Outgoing(String topic, String key, String payload, UUID announcesSaga,
                           UUID countsRetryFor) {
        public Outgoing(String topic, String key, String payload) {
            this(topic, key, payload, null, null);
        }

        public Outgoing(String topic, String key, String payload, UUID announcesSaga) {
            this(topic, key, payload, announcesSaga, null);
        }
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
            // the payload may carry an email in some other spelling — scrub before logging
            LOG.warn("dropping malformed event on {}: {}", topic, scrubbed(payload));
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

    /**
     * The timeout sweep — and the outbox's broom. Overdue sagas get their purge command resent
     * while retries remain; the exhausted ones compensate and announce the failure (naming the
     * participants that DID purge — a partial purge is worth knowing about); finished sagas whose
     * outcome never got its announced mark have it re-published.
     */
    public List<Outgoing> sweepOverdue() {
        SweepOverdue.Swept swept = sweep.execute(Instant.now(clock));
        List<Outgoing> out = new ArrayList<>();
        for (SagaStore.Retry retry : swept.retries()) {
            LOG.info("purge unconfirmed in time for {}; re-commanding (saga {})",
                    masked(retry.email()), retry.sagaId());
            // the retry carries no policy: the original fact is not persisted, so a re-commanded
            // purge falls back to the participants' defaults — accepted, the alternative is
            // storing the leaver's choices beside the saga. countsRetryFor makes the loop charge
            // the retry counter only once this command is proven delivered — an undeliverable
            // re-command burns nothing and the next sweep simply offers the candidate again
            out.add(purgeRetryCommand(retry.sagaId(), retry.email()));
        }
        for (SagaStore.Compensated failed : swept.compensated()) {
            LOG.warn("portal purge overdue for {} despite the retries; announcing the failure "
                    + "(saga {}, already purged: {})", masked(failed.email()), failed.sagaId(), failed.confirmed());
            out.add(outcome("PORTAL_PURGE_FAILED", failed.email(), failed.sagaId(), failed.confirmed()));
        }
        if (!swept.compensated().isEmpty()) {
            MetricsEndpoint.compensated(swept.compensated().size());
        }
        for (SagaStore.PendingOutcome pending : swept.unannounced()) {
            LOG.info("re-announcing the {} outcome for {} (saga {}): the first announcement "
                    + "never reached the broker", pending.state(), masked(pending.email()), pending.sagaId());
            out.add("COMPLETED".equals(pending.state())
                    ? outcome("PORTAL_CONTENT_PURGED", pending.email(), pending.sagaId(), null)
                    : outcome("PORTAL_PURGE_FAILED", pending.email(), pending.sagaId(), pending.confirmed()));
        }
        return out;
    }

    private List<Outgoing> onDeletionRequested(JsonNode fact) {
        String email = fact.path("email").asText();
        if (email.isBlank()) {
            LOG.warn("dropping deletion fact without an email: {}", summarised(fact));
            return List.of();
        }
        // the fact's id is the replay key: the same fact twice finds the same saga. A missing or
        // mangled id is a poison pill — inventing a random one would silently disable the replay
        // protection (and security's pact pins the uuid), so it drops like malformed JSON
        UUID factId;
        try {
            factId = UUID.fromString(fact.path("id").asText());
        } catch (IllegalArgumentException poison) {
            LOG.warn("dropping deletion fact with a missing or invalid id: {}", summarised(fact));
            return List.of();
        }
        BeginOffboarding.Begun begun = begin.execute(factId, email, Instant.now(clock));
        if (begun.completedImmediately()) {
            LOG.info("no content participants configured; portal instantly clean for {}", masked(email));
            return List.of(outcome("PORTAL_CONTENT_PURGED", email, begun.sagaId(), null));
        }
        LOG.info("commanding the content purge for {} (saga {})", masked(email), begun.sagaId());
        return List.of(purgeCommand(begun.sagaId(), email, fact.path("policy")));
    }

    private List<Outgoing> onConfirmation(JsonNode confirmation, String participant) {
        String email = confirmation.path("email").asText();
        if (email.isBlank()) {
            LOG.warn("dropping {} confirmation without an email: {}", participant, summarised(confirmation));
            return List.of();
        }
        // fresh confirmations echo the saga id from the command — the precise address, and the
        // store treats a stale one (saga no longer STARTED) as a stray from a closed case. ONLY
        // an absent field degrades to the email lookup (old producers). A field that is present
        // but unparseable drops like any poison pill: degrading it to the email lookup would
        // reopen by the back door exactly the hole the precise address closed — a mangled echo
        // of a finished case could land on a NEWER saga for the same account
        UUID sagaId = null;
        if (confirmation.hasNonNull("sagaId")) {
            try {
                sagaId = UUID.fromString(confirmation.get("sagaId").asText());
            } catch (IllegalArgumentException mangled) {
                LOG.warn("dropping {} confirmation with an unparseable sagaId: {}",
                        participant, summarised(confirmation));
                return List.of();
            }
        }
        Optional<UUID> completed = confirm.execute(email, sagaId, participant, Instant.now(clock));
        if (completed.isEmpty()) {
            LOG.info("recorded {} purge confirmation for {}; saga not complete yet",
                    participant, masked(email));
            return List.of();
        }
        LOG.info("all participants confirmed for {}; announcing the portal purged", masked(email));
        return List.of(outcome("PORTAL_CONTENT_PURGED", email, completed.get(), null));
    }

    private Outgoing purgeCommand(UUID sagaId, String email, JsonNode policy) {
        return new Outgoing(COMMANDS_TOPIC, email, purgePayload(sagaId, email, policy));
    }

    /**
     * The sweeper's re-command: byte-wise the same command as {@link #purgeCommand} (minus the
     * policy, see sweepOverdue), plus the {@code countsRetryFor} mark that lets the loop charge
     * the retry counter only after the broker demonstrably accepted it.
     */
    private Outgoing purgeRetryCommand(UUID sagaId, String email) {
        return new Outgoing(COMMANDS_TOPIC, email, purgePayload(sagaId, email, null), null, sagaId);
    }

    private String purgePayload(UUID sagaId, String email, JsonNode policy) {
        ObjectNode command = mapper.createObjectNode()
                .put("id", UUID.randomUUID().toString())
                .put("sagaId", sagaId.toString())
                .put("type", "PURGE_USER_CONTENT")
                .put("email", email)
                // envelope version (workspace ADR 0004): fields only ever added within version 1
                .put("version", 1);
        if (policy != null && policy.isObject()) {
            command.set("policy", policy);   // the leaver's choices, ferried untouched
        }
        return write(command);
    }

    private Outgoing outcome(String type, String email, UUID sagaId, java.util.Set<String> confirmed) {
        ObjectNode node = mapper.createObjectNode()
                // the id is DERIVED from (saga, type), never random: the sweeper may re-publish
                // an outcome the first announcement of which was lost, and a re-publication must
                // be byte-identical to the original so consumers deduplicate on the id
                .put("id", UUID.nameUUIDFromBytes(
                        (sagaId + "|" + type).getBytes(StandardCharsets.UTF_8)).toString())
                .put("type", type)
                .put("email", email)
                .put("version", 1);
        if (confirmed != null) {
            // the partial-purge disclosure: which participants DID purge before the failure —
            // sorted, keeping replays of the same outcome byte-identical here too
            ArrayNode names = node.putArray("confirmed");
            new TreeSet<>(confirmed).forEach(names::add);
        }
        return new Outgoing(OUTCOMES_TOPIC, email, write(node), sagaId);
    }

    /**
     * PII hygiene for the poison-pill WARNs: never the whole payload — just what places the
     * record (type + id) and a masked hint of any email it carried.
     */
    private static String summarised(JsonNode event) {
        StringBuilder summary = new StringBuilder("type=").append(event.path("type").asText("?"))
                .append(", id=").append(event.path("id").asText("?"));
        if (event.hasNonNull("email") && !event.path("email").asText().isBlank()) {
            summary.append(", email=").append(masked(event.path("email").asText()));
        }
        return summary.toString();
    }

    /** For payloads that would not even parse: mask anything shaped like an email address. */
    private static String scrubbed(String payload) {
        return payload.replaceAll("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", "***");
    }

    /** PII hygiene: INFO-level logs carry only a hint of the address, enough to follow one saga. */
    private static String masked(String email) {
        int at = email.indexOf('@');
        if (at <= 2) {
            return "***" + (at < 0 ? "" : email.substring(at));
        }
        return email.substring(0, 2) + "***" + email.substring(at);
    }

    private String write(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (Exception impossible) {
            throw new IllegalStateException("could not serialise event", impossible);
        }
    }
}
