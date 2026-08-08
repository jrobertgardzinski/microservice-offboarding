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
 * vocabulary belongs to the content services, this one only ferries — and are stored with the
 * saga so the sweeper's re-command repeats them instead of a bare default.
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
     * The three commands this orchestrator sends its participants, and the whole reason the saga
     * has a compensation worth the name.
     *
     * <p>{@link #MARK_COMMAND} is unchanged on the wire (the participants' pacts pin it) but no
     * longer means "destroy": a participant marks the leaver's content, which takes it out of every
     * public read and destroys nothing. Its confirmation — also unchanged — therefore now means
     * "reserved", and a reservation is a thing that can be given back.
     *
     * <p>{@link #ERASE_COMMAND} is the CLOSURE: sent once every required participant has confirmed,
     * so once the case can no longer fail. It is what actually deletes, and past it the saga has
     * nothing left but retrying.
     *
     * <p>{@link #RESTORE_COMMAND} is the compensation: sent when the case is given up on, so the
     * marks come off and the content is public again. It goes to EVERY participant, not only to the
     * ones that confirmed — a participant may have marked and had its confirmation lost, and
     * restoring what was never marked is a no-op by construction.
     */
    public static final String MARK_COMMAND = "PURGE_USER_CONTENT";
    public static final String ERASE_COMMAND = "ERASE_USER_CONTENT";
    public static final String RESTORE_COMMAND = "RESTORE_USER_CONTENT";

    /**
     * The cap on the leaver's policy object, serialised (UTF-8 bytes): this service only FERRIES
     * the policy — it never reads it — so an absurdly large blob would ride into every saga row
     * (stored verbatim for the sweeper's re-command) and every re-commanded purge without limit.
     * Past the cap the saga proceeds WITHOUT the policy (the participants' defaults), exactly
     * like an unreadable stored policy: WARN, never a dropped saga and never a wedge.
     */
    static final int MAX_POLICY_BYTES = 8 * 1024;

    /**
     * An event to publish: the loop adds the correlation-id header and sends. When the event is a
     * saga's outcome, {@code announcesSaga} names it so the loop can mark the outbox after a
     * successful flush; when it is the sweeper RE-commanding an overdue purge,
     * {@code countsRetryFor} names the saga whose retry counter the loop charges once the send is
     * proven delivered ({@link com.jrobertgardzinski.offboarding.application.SagaStore#retryDelivered}).
     * Everything else leaves both null.
     */
    public record Outgoing(String topic, String key, String payload, UUID announcesSaga,
                           UUID countsRetryFor, UUID partOfSaga) {
        public Outgoing(String topic, String key, String payload) {
            this(topic, key, payload, null, null, null);
        }

        public Outgoing(String topic, String key, String payload, UUID announcesSaga) {
            this(topic, key, payload, announcesSaga, null, announcesSaga);
        }

        public Outgoing(String topic, String key, String payload, UUID announcesSaga,
                        UUID countsRetryFor) {
            this(topic, key, payload, announcesSaga, countsRetryFor,
                    announcesSaga != null ? announcesSaga : countsRetryFor);
        }

        /**
         * The saga this event belongs to for the purposes of the outbox mark — which is NOT the
         * same question as "does it announce the outcome". A closure or a compensation command is
         * published in the same breath as the verdict it accompanies, and if it fails to reach the
         * broker while the verdict lands, marking the saga announced would seal the case with one
         * of its two messages missing and nothing left to re-publish it. Naming the saga here lets
         * the loop withhold the mark from ALL of a saga's events when ANY of them is undelivered.
         */
        public static Outgoing command(String topic, String key, String payload, UUID sagaId) {
            return new Outgoing(topic, key, payload, null, null, sagaId);
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
            // the retry repeats the ORIGINAL command: the leaver's policy choices were stored
            // with the saga at start (V3) precisely so a re-commanded purge does not silently
            // fall back to the participants' defaults. countsRetryFor makes the loop charge
            // the retry counter only once this command is proven delivered — an undeliverable
            // re-command burns nothing and the next sweep simply offers the candidate again
            out.add(purgeRetryCommand(retry.sagaId(), retry.email(), retry.policy()));
        }
        for (SagaStore.Compensated failed : swept.compensated()) {
            LOG.warn("portal purge overdue for {} despite the retries; compensating and announcing "
                            + "the failure (saga {}, already marked: {})",
                    masked(failed.email()), failed.sagaId(), failed.confirmed());
            // the compensation goes out FIRST, and it is what makes this word honest: before the
            // participants learnt to mark instead of destroy, "compensated" meant nothing but an
            // apology — the content was already gone. It is sent to every participant, not only to
            // the ones that confirmed: a mark whose confirmation was lost is still a mark
            out.add(participantCommand(RESTORE_COMMAND, failed.sagaId(), failed.email(), null));
            out.add(outcome("PORTAL_PURGE_FAILED", failed.email(), failed.sagaId(),
                    failed.securitySagaId(), failed.confirmed()));
        }
        if (!swept.compensated().isEmpty()) {
            MetricsEndpoint.compensated(swept.compensated().size());
        }
        for (SagaStore.PendingOutcome pending : swept.unannounced()) {
            LOG.info("re-announcing the {} outcome for {} (saga {}): the first announcement "
                    + "never reached the broker", pending.state(), masked(pending.email()), pending.sagaId());
            // the participants' command rides along with every re-announcement, because the two
            // went out together and are withheld together: a saga is marked announced only when
            // ALL of its events reached the broker (KafkaLoop#settleDeliveries), so an outcome
            // still unannounced means the closure (or the compensation) may be missing too. Both
            // are idempotent at the participants, so repeating a delivered one costs nothing —
            // whereas losing a closure leaves content hidden for ever and nobody the wiser
            boolean completed = "COMPLETED".equals(pending.state());
            out.add(participantCommand(completed ? ERASE_COMMAND : RESTORE_COMMAND,
                    pending.sagaId(), pending.email(), completed ? pending.policy() : null));
            out.add(completed
                    ? outcome("PORTAL_CONTENT_PURGED", pending.email(), pending.sagaId(),
                    pending.securitySagaId(), null)
                    : outcome("PORTAL_PURGE_FAILED", pending.email(), pending.sagaId(),
                    pending.securitySagaId(), pending.confirmed()));
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
        // security's own handle on the deletion, stored with the saga and ECHOED by the verdict:
        // without it security can only match the verdict by email address, and a late verdict of
        // a closed case then compensates a NEWER deletion for the same address. Absent means an
        // older producer — the saga opens without a handle, as every saga did before this field.
        // Present-but-mangled drops like the fact's own id: the deletion cannot be correlated, so
        // purging the content would risk a verdict nobody can match (content erased, account
        // restored), and security's own timeout still unlocks the account
        UUID securitySagaId = null;
        if (fact.has("sagaId")) {
            String raw = fact.get("sagaId").asText();
            try {
                securitySagaId = UUID.fromString(raw);
            } catch (IllegalArgumentException mangled) {
                LOG.warn("dropping deletion fact with an unparseable sagaId \"{}\": {}",
                        raw, summarised(fact));
                return List.of();
            }
        }
        // the policy is stored with the saga (verbatim, only when it is the object the command
        // would carry) so the sweeper's re-command can repeat the original command — capped at
        // MAX_POLICY_BYTES: an oversized blob is dropped from the saga AND the command alike
        // (keeping the two identical, so the re-command still repeats the original), and the
        // purge proceeds on the participants' defaults, like with an unreadable stored policy
        JsonNode policy = fact.path("policy").isObject() ? fact.path("policy") : null;
        String storedPolicy = policy == null ? null : write(policy);
        if (storedPolicy != null
                && storedPolicy.getBytes(StandardCharsets.UTF_8).length > MAX_POLICY_BYTES) {
            LOG.warn("policy on the deletion fact serialises to {} bytes, over the {}-byte cap;"
                            + " starting the saga without it — the purge will use the"
                            + " participants' defaults ({})",
                    storedPolicy.getBytes(StandardCharsets.UTF_8).length, MAX_POLICY_BYTES,
                    summarised(fact));
            policy = null;
            storedPolicy = null;
        }
        BeginOffboarding.Begun begun =
                begin.execute(factId, email, storedPolicy, securitySagaId, Instant.now(clock));
        if (begun.nothingToPurge()) {
            if (!begun.completedNow()) {
                // the once-latch said no: this fact is a replay and the saga finished long ago.
                // Announcing again would put a second verdict on the wire for a settled case —
                // whatever it still owes is the outbox's business (unannouncedOutcomes), not this
                // fact's
                LOG.info("replayed deletion fact for {}: nothing to purge and the saga is already"
                        + " finished; no second announcement (saga {})", masked(email), begun.sagaId());
                return List.of();
            }
            LOG.info("no content participants configured; portal instantly clean for {}", masked(email));
            return List.of(outcome("PORTAL_CONTENT_PURGED", email, begun.sagaId(),
                    securitySagaId, null));
        }
        LOG.info("commanding the content purge for {} (saga {})", masked(email), begun.sagaId());
        return List.of(purgeCommand(begun.sagaId(), email, policy));
    }

    private List<Outgoing> onConfirmation(JsonNode confirmation, String participant) {
        String email = confirmation.path("email").asText();
        if (email.isBlank()) {
            LOG.warn("dropping {} confirmation without an email: {}", participant, summarised(confirmation));
            return List.of();
        }
        // fresh confirmations echo the saga id from the command — the precise address, and the
        // store treats a stale one (saga no longer STARTED) as a stray from a closed case. ONLY
        // an ABSENT field degrades to the email lookup (old producers). A field that is present
        // but null or unparseable drops like any poison pill: a producer that WROTE the field
        // and failed to fill it is mangled, not old, and degrading it to the email lookup would
        // reopen by the back door exactly the hole the precise address closed — a mangled echo
        // of a finished case could land on a NEWER saga for the same account
        UUID sagaId = null;
        if (confirmation.has("sagaId")) {
            // the raw value is safe to log verbatim: a saga id is a correlation handle, not PII —
            // and it is exactly what an operator needs to trace the mangled producer
            String raw = confirmation.get("sagaId").asText();
            try {
                sagaId = UUID.fromString(raw);
            } catch (IllegalArgumentException mangled) {
                LOG.warn("dropping {} confirmation with an unparseable sagaId \"{}\": {}",
                        participant, raw, summarised(confirmation));
                return List.of();
            }
        }
        Optional<SagaStore.Recorded> landed =
                confirm.execute(email, sagaId, participant, Instant.now(clock));
        // two outcomes that used to share one line, and they are not the same event: a stray
        // recorded NOTHING (no saga is waiting for it — a closed case, or an account with no
        // deletion under way), while a recorded confirmation is progress. Logging both as
        // "recorded ... saga not complete yet" told an operator chasing a stuck deletion that the
        // confirmation had been stored, when it had been dropped
        if (landed.isEmpty()) {
            LOG.info("dropping stray {} purge confirmation for {}: no saga is waiting for it",
                    participant, masked(email));
            return List.of();
        }
        if (!landed.get().completedSaga()) {
            LOG.info("recorded {} purge confirmation for {}; saga not complete yet",
                    participant, masked(email));
            return List.of();
        }
        LOG.info("all participants confirmed the mark for {}; closing the saga (the erasure is"
                + " commanded now) and announcing the portal purged", masked(email));
        // ORDER MATTERS, and not for the reason it looks like. The closure is what makes the
        // erasure real, and the verdict is what lets security delete the account; publishing the
        // closure first means the irreversible step is on the wire before anybody is told the
        // portal is clean. Neither is marked announced until BOTH have reached the broker, so a
        // half-published pair is simply re-published by the next sweep.
        return List.of(
                participantCommand(ERASE_COMMAND, landed.get().sagaId(), email,
                        landed.get().policy()),
                outcome("PORTAL_CONTENT_PURGED", email, landed.get().sagaId(),
                        landed.get().securitySagaId(), null));
    }

    private Outgoing purgeCommand(UUID sagaId, String email, JsonNode policy) {
        return new Outgoing(COMMANDS_TOPIC, email, commandPayload(MARK_COMMAND, sagaId, email, policy));
    }

    /**
     * The closure and the compensation, in the SAME envelope as the mark they end — same topic,
     * same key, same fields, only the {@code type} differs, so a participant routes all three from
     * one listener and an operator reading the topic sees one conversation.
     *
     * <p>The closure carries the leaver's stored policy: the participants apply their rule (delete,
     * anonymise, keep the popular ones) at ERASURE time, not at mark time, because the rule reads
     * vote scores and the leaver's own votes only go with the erasure. The compensation carries
     * none — putting content back needs no policy.
     *
     * <p>{@code partOfSaga} rather than {@code announcesSaga}: this event does not announce the
     * outcome, but it must share the outcome's fate at the outbox (see {@link Outgoing#command}).
     */
    private Outgoing participantCommand(String type, UUID sagaId, String email, String storedPolicy) {
        return Outgoing.command(COMMANDS_TOPIC, email,
                commandPayload(type, sagaId, email, storedPolicy(sagaId, storedPolicy)), sagaId);
    }

    /**
     * The sweeper's re-command: the SAME shape as {@link #purgeCommand}, built by the same
     * {@link #commandPayload} — including the leaver's policy choices, restored verbatim from the
     * saga — plus the {@code countsRetryFor} mark that lets the loop charge the retry counter
     * only after the broker demonstrably accepted it.
     */
    private Outgoing purgeRetryCommand(UUID sagaId, String email, String storedPolicy) {
        return new Outgoing(COMMANDS_TOPIC, email,
                commandPayload(MARK_COMMAND, sagaId, email, storedPolicy(sagaId, storedPolicy)),
                null, sagaId);
    }

    /** The stored policy back into the node {@link #commandPayload} ferries; null stays null. */
    private JsonNode storedPolicy(UUID sagaId, String storedPolicy) {
        if (storedPolicy == null) {
            return null;
        }
        try {
            return mapper.readTree(storedPolicy);
        } catch (Exception unreadable) {
            // stored by us off a parsed fact, so this cannot really happen — but a re-command
            // with the participants' defaults still beats a wedged sweeper pass
            LOG.warn("stored policy for saga {} is unreadable; re-commanding without it", sagaId);
            return null;
        }
    }

    private String commandPayload(String type, UUID sagaId, String email, JsonNode policy) {
        ObjectNode command = mapper.createObjectNode()
                .put("id", UUID.randomUUID().toString())
                .put("sagaId", sagaId.toString())
                .put("type", type)
                .put("email", email)
                // envelope version (workspace ADR 0004): fields only ever added within version 1
                .put("version", 1);
        if (policy != null && policy.isObject()) {
            command.set("policy", policy);   // the leaver's choices, ferried untouched
        }
        return write(command);
    }

    /**
     * The single verdict security waits for. {@code securitySagaId} is SECURITY's handle, echoed
     * from the fact that opened this case (null for a saga that never got one) — the correlation
     * that lets security settle THE deletion this verdict is about instead of matching by email
     * address, where a late verdict of a closed case compensates a newer request for the same
     * person. The field is spelled {@code sagaId} on the wire, exactly as on the fact: on this
     * boundary that name means "the saga in SECURITY's terms", while on the participants'
     * boundary (the purge command and its confirmation) it means the portal's own. Additive
     * within envelope version 1 (workspace ADR 0004).
     */
    private Outgoing outcome(String type, String email, UUID sagaId, UUID securitySagaId,
                             java.util.Set<String> confirmed) {
        ObjectNode node = mapper.createObjectNode()
                // the id is DERIVED from (saga, type), never random: the sweeper may re-publish
                // an outcome the first announcement of which was lost, and a re-publication must
                // be byte-identical to the original so consumers deduplicate on the id
                .put("id", UUID.nameUUIDFromBytes(
                        (sagaId + "|" + type).getBytes(StandardCharsets.UTF_8)).toString())
                .put("type", type)
                .put("email", email)
                .put("version", 1);
        if (securitySagaId != null) {
            node.put("sagaId", securitySagaId.toString());
        }
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
