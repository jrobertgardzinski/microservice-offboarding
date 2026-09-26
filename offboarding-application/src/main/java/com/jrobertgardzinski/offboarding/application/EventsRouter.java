package com.jrobertgardzinski.offboarding.application;

import com.jrobertgardzinski.closure.ClosureInitiator;
import com.jrobertgardzinski.closure.ClosureMessages;
import java.util.stream.Stream;
import com.jrobertgardzinski.util.constraint.Outcome;
import com.jrobertgardzinski.envelope.Masked;
import com.jrobertgardzinski.envelope.Envelope;
import com.jrobertgardzinski.offboarding.domain.Compensated;
import com.jrobertgardzinski.offboarding.domain.PendingOutcome;
import com.jrobertgardzinski.offboarding.domain.Recorded;
import com.jrobertgardzinski.offboarding.domain.Retry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jrobertgardzinski.offboarding.system.BeginOffboarding;
import com.jrobertgardzinski.offboarding.system.RecordConfirmation;
import com.jrobertgardzinski.offboarding.domain.Observation;
import com.jrobertgardzinski.observation.Observations;
import com.jrobertgardzinski.offboarding.system.SagaStore;
import com.jrobertgardzinski.offboarding.system.SweepOverdue;
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
 * {@code USER_CONTENT_PURGED} confirmations (each named by the {@link Source} it came from — the
 * adapter decides what a source is). Out: the {@code PURGE_USER_CONTENT} command — byte-compatible with the one
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
    // the names themselves live in account-closure, where identity and the participants read the
    // same ones; these stay as this router's own vocabulary so the code below still reads as a saga
    public static final String MARK_COMMAND = ClosureMessages.PURGE_USER_CONTENT;
    public static final String ERASE_COMMAND = ClosureMessages.ERASE_USER_CONTENT;
    public static final String RESTORE_COMMAND = ClosureMessages.RESTORE_USER_CONTENT;

    /**
     * The cap on the leaver's policy object, serialised (UTF-8 bytes): this service only FERRIES
     * the policy — it never reads it — so an absurdly large blob would ride into every saga row
     * (stored verbatim for the sweeper's re-command) and every re-commanded purge without limit.
     * Past the cap the saga proceeds WITHOUT the policy (the participants' defaults), exactly
     * like an unreadable stored policy: WARN, never a dropped saga and never a wedge.
     */
    static final int MAX_POLICY_BYTES = 8 * 1024;

    /**
     * The retry counter's address: the saga to charge, and the count the sweep offered the
     * candidate at. Both, because the charge is conditional on the round it pays for — two
     * sweepers holding the same overdue saga must not buy two retries with one re-command (see
     * {@link com.jrobertgardzinski.offboarding.system.SagaStore#retryDelivered}).
     */
    public record RetryCharge(UUID sagaId, int retriesSoFar) {
    }

    /**
     * An event to publish: the loop adds the correlation-id header and sends. When the event is a
     * saga's outcome, {@code announcesSaga} names it so the loop can mark the outbox after a
     * successful flush; when it is the sweeper RE-commanding an overdue purge,
     * {@code countsRetryFor} names the saga whose retry counter the loop charges once the send is
     * proven delivered ({@link com.jrobertgardzinski.offboarding.system.SagaStore#retryDelivered}).
     * Everything else leaves both null.
     */
    public record Outgoing(Destination destination, String key, String payload, UUID announcesSaga,
                           RetryCharge countsRetryFor, UUID partOfSaga) {
        public Outgoing(Destination destination, String key, String payload) {
            this(destination, key, payload, null, null, null);
        }

        public Outgoing(Destination destination, String key, String payload, UUID announcesSaga) {
            this(destination, key, payload, announcesSaga, null, announcesSaga);
        }

        public Outgoing(Destination destination, String key, String payload, UUID announcesSaga,
                        RetryCharge countsRetryFor) {
            this(destination, key, payload, announcesSaga, countsRetryFor,
                    announcesSaga != null ? announcesSaga
                            : countsRetryFor == null ? null : countsRetryFor.sagaId());
        }

        /**
         * The saga this event belongs to for the purposes of the outbox mark — which is NOT the
         * same question as "does it announce the outcome". A closure or a compensation command is
         * published in the same breath as the verdict it accompanies, and if it fails to reach the
         * broker while the verdict lands, marking the saga announced would seal the case with one
         * of its two messages missing and nothing left to re-publish it. Naming the saga here lets
         * the loop withhold the mark from ALL of a saga's events when ANY of them is undelivered.
         */
        public static Outgoing command(Destination destination, String key, String payload,
                                       UUID sagaId) {
            return new Outgoing(destination, key, payload, null, null, sagaId);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(EventsRouter.class);

    private final BeginOffboarding begin;
    private final RecordConfirmation confirm;
    private final SweepOverdue sweep;
    private final ObjectMapper mapper;
    private final Clock clock;
    /** Where this router STATES what it noticed; the adapter decides these are counters. */
    private final Observations<Observation> observations;

    public EventsRouter(BeginOffboarding begin, RecordConfirmation confirm, SweepOverdue sweep,
                        ObjectMapper mapper, Clock clock) {
        this(begin, confirm, sweep, mapper, clock, Observations.<Observation>silent());
    }

    /**
     * The same router with somebody listening. Kept as a second constructor rather than a required
     * argument because the facts are the caller's to collect: every test here drives the router
     * unwatched, which is the boundary this design promises and the cheapest possible proof of it.
     */
    public EventsRouter(BeginOffboarding begin, RecordConfirmation confirm, SweepOverdue sweep,
                        ObjectMapper mapper, Clock clock, Observations<Observation> observations) {
        this.observations = observations;
        this.begin = begin;
        this.confirm = confirm;
        this.sweep = sweep;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * Route one message to its use case; returns what to say in response, and to whom.
     *
     * <p>{@link Source} rather than a topic name, because which arrival is whose is the
     * deployment's shape and not the saga's: between services an adapter reads it off a topic, in
     * one process off whichever module handed the message over. While this method took a topic, the
     * orchestrator held the topology — and a monolith would have had to invent Kafka topics for a
     * saga talking to itself.
     */
    public List<Outgoing> handle(Source source, String payload) {
        Outcome<Envelope> read = Envelope.read(payload, mapper);
        if (read.findValue().isEmpty()) {
            // its SIZE, never its text: an unparsed payload is of unknown shape, so the scrubber
            // has the least idea what it is looking at, and a newline in it would become a new log
            // LINE — a forged ERROR planted in an operator's view
            LOG.warn("dropping an unreadable message from {} ({}): {}", source,
                    Masked.sizeOf(payload), read.errorCodes());
            return List.of();
        }
        Envelope envelope = read.findValue().orElseThrow();
        String type = envelope.type();
        return switch (source) {
            case Source.Security ignored when ClosureMessages.ACCOUNT_DELETION_REQUESTED.equals(type) ->
                    onDeletionRequested(envelope);
            case Source.Participant participant when ClosureMessages.USER_CONTENT_PURGED.equals(type) ->
                    onConfirmation(envelope, participant.name());
            // the other lifecycle events of whoever sent this share the same road; not ours
            default -> List.of();
        };
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
        for (Retry retry : swept.retries()) {
            LOG.info("purge unconfirmed in time for {}; re-commanding (saga {})",
                    Masked.address(retry.email()), retry.sagaId());
            // the retry repeats the ORIGINAL command: the leaver's policy choices were stored
            // with the saga at start (V3) precisely so a re-commanded purge does not silently
            // fall back to the participants' defaults. countsRetryFor makes the loop charge
            // the retry counter only once this command is proven delivered — an undeliverable
            // re-command burns nothing and the next sweep simply offers the candidate again
            out.add(purgeRetryCommand(retry.sagaId(), retry.userId(), retry.policy(),
                    retry.initiatedBy(), retry.retriesSoFar()));
        }
        for (Compensated failed : swept.compensated()) {
            LOG.warn("portal purge overdue for {} despite the retries; compensating and announcing "
                            + "the failure (saga {}, already marked: {})",
                    Masked.address(failed.email()), failed.sagaId(), failed.confirmed());
            // the compensation goes out FIRST, and it is what makes this word honest: before the
            // participants learnt to mark instead of destroy, "compensated" meant nothing but an
            // apology — the content was already gone. It is sent to every participant, not only to
            // the ones that confirmed: a mark whose confirmation was lost is still a mark
            // no policy and no basis: putting content BACK applies no rule, so neither field
            // has anything to decide here (the payload states SELF, the safe reading of null)
            out.add(participantCommand(RESTORE_COMMAND, failed.sagaId(), failed.userId(), null, null));
            out.add(outcome(ClosureMessages.PORTAL_PURGE_FAILED, failed.email(), failed.sagaId(),
                    failed.securitySagaId(), failed.confirmed()));
        }
        if (!swept.compensated().isEmpty()) {
            observations.record(new Observation.SagaCompensated(swept.compensated().size()));
        }
        for (PendingOutcome pending : swept.unannounced()) {
            LOG.info("re-announcing the {} outcome for {} (saga {}): the first announcement "
                    + "never reached the broker", pending.state(), Masked.address(pending.email()), pending.sagaId());
            // the participants' command rides along with every re-announcement, because the two
            // went out together and are withheld together: a saga is marked announced only when
            // ALL of its events reached the broker (KafkaLoop#settleDeliveries), so an outcome
            // still unannounced means the closure (or the compensation) may be missing too. Both
            // are idempotent at the participants, so repeating a delivered one costs nothing —
            // whereas losing a closure leaves content hidden for ever and nobody the wiser
            boolean completed = "COMPLETED".equals(pending.state());
            out.add(participantCommand(completed ? ERASE_COMMAND : RESTORE_COMMAND,
                    pending.sagaId(), pending.userId(),
                    completed ? pending.policy() : null, pending.initiatedBy()));
            out.add(completed
                    ? outcome(ClosureMessages.PORTAL_CONTENT_PURGED, pending.email(), pending.sagaId(),
                    pending.securitySagaId(), null)
                    : outcome(ClosureMessages.PORTAL_PURGE_FAILED, pending.email(), pending.sagaId(),
                    pending.securitySagaId(), pending.confirmed()));
        }
        return out;
    }

    /**
     * Everything this method used to do before it could think: parse, refuse a poison pill, cap a
     * ferried blob, keep an address out of the log. All of that is the envelope's job now, and what
     * is left below the reads is the saga's own decision.
     *
     * <p>The reasons survived the move; only their code did not. A missing or mangled {@code id} is
     * a poison pill because that id is the REPLAY KEY — inventing one would silently disable the
     * protection that makes a redelivered fact find its own saga. An absent {@code sagaId} is an
     * older producer and degrades; a mangled one drops, because a correlation nobody can match
     * would let a verdict settle the wrong deletion. An oversized {@code policy} is neither: the
     * deletion goes ahead without it.
     */
    private List<Outgoing> onDeletionRequested(Envelope fact) {
        Outcome<String> readEmail = fact.requiredText("email");
        Outcome<UUID> readFactId = fact.requiredUuid("id");
        Outcome<Optional<UUID>> readSecuritySagaId = fact.optionalUuid("sagaId");
        Outcome<Optional<JsonNode>> readPolicy = fact.objectWithin("policy", MAX_POLICY_BYTES);
        Outcome<Optional<UUID>> readUserId = fact.optionalUuid(ClosureMessages.Field.USER_ID);
        List<String> refusals = Stream.of(readEmail, readFactId, readSecuritySagaId)
                .flatMap(outcome -> outcome.errorCodes().stream()).toList();
        if (!refusals.isEmpty()) {
            LOG.warn("dropping a deletion fact this service cannot place ({}): {}",
                    refusals, fact.summary());
            return List.of();
        }
        if (!readPolicy.errorCodes().isEmpty() || readPolicy instanceof Outcome.AllowedWithWarning) {
            LOG.warn("the policy on this deletion fact is over the {}-byte cap; starting the saga"
                            + " without it — the purge will use the participants' defaults ({})",
                    MAX_POLICY_BYTES, fact.summary());
        }
        String email = readEmail.findValue().orElseThrow();
        UUID factId = readFactId.findValue().orElseThrow();
        UUID securitySagaId = readSecuritySagaId.findValue().orElseThrow().orElse(null);
        // the leaver's identity is what every command and confirmation is keyed by: a fact
        // without one names nobody the content services could act for
        UUID userId = readUserId.findValue().orElse(Optional.empty()).orElse(null);
        if (userId == null) {
            LOG.warn("dropping a deletion fact without a user id ({}): {}", readUserId.errorCodes(), fact.summary());
            return List.of();
        }
        // who asked, normalised: anything that is not exactly ADMIN is the account's own owner,
        // which is also the honest reading of a fact from before the field existed — until then
        // security had one deletion route and only the owner could walk it
        String initiatedBy = ClosureInitiator.of(fact.node().path(ClosureMessages.Field.INITIATED_BY).asText()).wire();
        JsonNode policy = readPolicy.findValue().orElseThrow().orElse(null);
        String storedPolicy = policy == null ? null : write(policy);
        BeginOffboarding.Begun begun = begin.execute(factId, email, storedPolicy, securitySagaId,
                initiatedBy, userId, Instant.now(clock));
        if (begun.nothingToPurge()) {
            if (!begun.completedNow()) {
                // the once-latch said no, and there are two ways it can: the fact is a replay and
                // the saga finished long ago, or the saga this fact joined recorded a quorum this
                // participant-less configuration has no business declaring reached. Either way
                // nothing is announced here — a settled case owes at most a re-publication, which
                // is the outbox's business (unannouncedOutcomes), and a case still collecting is
                // the sweeper's
                LOG.info("deletion fact for {}: nothing to purge here, and this saga is not this"
                        + " call's to complete; no announcement (saga {})", Masked.address(email),
                        begun.sagaId());
                return List.of();
            }
            LOG.info("no content participants configured; portal instantly clean for {}", Masked.address(email));
            return List.of(outcome(ClosureMessages.PORTAL_CONTENT_PURGED, email, begun.sagaId(),
                    securitySagaId, null));
        }
        LOG.info("commanding the content purge for {} (saga {}, requested by {})", Masked.address(email),
                begun.sagaId(), initiatedBy);
        return List.of(purgeCommand(begun.sagaId(), userId, policy, initiatedBy));
    }

    /**
     * The same move as the fact above: the reads are the envelope's, the decision is the saga's.
     *
     * <p>The rule kept intact through it — and it is subtle enough to be worth stating twice — is
     * that an ABSENT {@code sagaId} degrades to the email lookup (an older producer) while a
     * PRESENT but unreadable one drops. A producer that wrote the field and failed to fill it is
     * mangled, not old, and degrading there would reopen by the back door exactly the hole the
     * precise address closed: a mangled echo of a finished case landing on a NEWER saga for the
     * same account.
     */
    private List<Outgoing> onConfirmation(Envelope confirmation, String participant) {
        Outcome<UUID> readUserId = confirmation.requiredUuid(ClosureMessages.Field.USER_ID);
        Outcome<Optional<UUID>> readSagaId = confirmation.optionalUuid("sagaId");
        List<String> refusals = Stream.of(readUserId, readSagaId)
                .flatMap(outcome -> outcome.errorCodes().stream()).toList();
        if (!refusals.isEmpty()) {
            LOG.warn("dropping a {} confirmation this service cannot place ({}): {}",
                    participant, refusals, confirmation.summary());
            return List.of();
        }
        UUID userId = readUserId.findValue().orElseThrow();
        UUID sagaId = readSagaId.findValue().orElseThrow().orElse(null);
        Optional<Recorded> landed =
                confirm.execute(userId, sagaId, participant, Instant.now(clock));
        // two outcomes that used to share one line, and they are not the same event: a stray
        // recorded NOTHING (no saga is waiting for it — a closed case, or an account with no
        // deletion under way), while a recorded confirmation is progress. Logging both as
        // "recorded ... saga not complete yet" told an operator chasing a stuck deletion that the
        // confirmation had been stored, when it had been dropped
        if (landed.isEmpty()) {
            LOG.info("dropping stray {} purge confirmation for user {}: no saga is waiting for it",
                    participant, userId);
            return List.of();
        }
        if (!landed.get().completedSaga()) {
            LOG.info("recorded {} purge confirmation for user {}; saga not complete yet",
                    participant, userId);
            return List.of();
        }
        LOG.info("all participants confirmed the mark for user {}; closing the saga (the erasure is"
                + " commanded now) and announcing the portal purged", userId);
        String email = landed.get().email();   // the verdict still names the account to identity
        // ORDER MATTERS, and not for the reason it looks like. The closure is what makes the
        // erasure real, and the verdict is what lets security delete the account; publishing the
        // closure first means the irreversible step is on the wire before anybody is told the
        // portal is clean. Neither is marked announced until BOTH have reached the broker, so a
        // half-published pair is simply re-published by the next sweep.
        return List.of(
                participantCommand(ERASE_COMMAND, landed.get().sagaId(), landed.get().userId(),
                        landed.get().policy(), landed.get().initiatedBy()),
                outcome(ClosureMessages.PORTAL_CONTENT_PURGED, email, landed.get().sagaId(),
                        landed.get().securitySagaId(), null));
    }

    private Outgoing purgeCommand(UUID sagaId, UUID userId, JsonNode policy, String initiatedBy) {
        return new Outgoing(Destination.PARTICIPANTS, keyOf(userId),
                commandPayload(MARK_COMMAND, sagaId, userId, policy, initiatedBy));
    }

    /** The partition key of a participant command: the leaver, so one person's commands stay ordered. */
    private static String keyOf(UUID userId) {
        return userId == null ? "" : userId.toString();
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
    private Outgoing participantCommand(String type, UUID sagaId, UUID userId, String storedPolicy,
                                        String initiatedBy) {
        return Outgoing.command(Destination.PARTICIPANTS, keyOf(userId),
                commandPayload(type, sagaId, userId, storedPolicy(sagaId, storedPolicy), initiatedBy),
                sagaId);
    }

    /**
     * The sweeper's re-command: the SAME shape as {@link #purgeCommand}, built by the same
     * {@link #commandPayload} — including the leaver's policy choices, restored verbatim from the
     * saga — plus the {@code countsRetryFor} mark that lets the loop charge the retry counter
     * only after the broker demonstrably accepted it.
     */
    private Outgoing purgeRetryCommand(UUID sagaId, UUID userId, String storedPolicy,
                                       String initiatedBy, int retriesSoFar) {
        return new Outgoing(Destination.PARTICIPANTS, keyOf(userId),
                commandPayload(MARK_COMMAND, sagaId, userId, storedPolicy(sagaId, storedPolicy),
                        initiatedBy),
                null, new RetryCharge(sagaId, retriesSoFar));
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

    private String commandPayload(String type, UUID sagaId, UUID userId, JsonNode policy,
                                  String initiatedBy) {
        ObjectNode command = mapper.createObjectNode()
                .put("id", UUID.randomUUID().toString())
                .put("sagaId", sagaId.toString())
                .put("type", type)
                // the basis, on ALL THREE commands and not only on the one that applies a rule:
                // one envelope, so a participant routes them from a single listener and an
                // operator reading the topic sees one conversation. Null for a saga opened before
                // the column existed, which the participants read exactly as SELF
                .put(ClosureMessages.Field.INITIATED_BY,
                        initiatedBy == null ? ClosureInitiator.SELF.wire() : initiatedBy)
                // envelope version (workspace ADR 0004): fields only ever added within version 1
                .put("version", 1);
        if (userId != null) {
            // the leaver, by identity — the only key a command carries; a saga opened before the
            // cutover has none, and the participants drop such a command as addressed to nobody
            command.put(ClosureMessages.Field.USER_ID, userId.toString());
        }
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
        return new Outgoing(Destination.SECURITY, email, write(node), sagaId);
    }




    private String write(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (Exception impossible) {
            throw new IllegalStateException("could not serialise event", impossible);
        }
    }
}
