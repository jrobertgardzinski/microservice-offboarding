package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SagaStore;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * {@link KafkaLoop} against a REAL broker (Testcontainers, the same apache/kafka the compose
 * stack runs) — pinning the transport guarantees the unit tests cannot see: the flush-before-
 * commit ordering that makes the outbox honest, the offset moving PAST a dropped poison pill so a
 * restart never replays it, the rewind-and-backoff that survives an infrastructure outage without
 * losing or double-processing records, the sweeper re-announcing an outcome a crash orphaned, and
 * /health's stall detection once a loop stops passing. Saga state lives in the real
 * {@link JdbcSagaStore} over the dev H2 — the same Flyway schema as production, so the
 * outcome_announced outbox column is exercised end to end.
 *
 * <p>One broker per class (container start dwarfs every test); per-test input topics keep the
 * loops' subscriptions disjoint, and per-test emails keep the shared command/outcome topics
 * readable. Skipped where docker is absent, like memes' MinIO round-trip.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaLoopIntegrationTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
                    // join new consumer groups immediately; the default 3s delay only adds flake room
                    .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

    /** The loop's hardcoded consumer group (see KafkaLoop.consumerProps). */
    private static final String LOOP_GROUP = "offboarding";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration GENEROUS = Duration.ofSeconds(30);

    private static KafkaProducer<String, String> testProducer;
    private static AdminClient admin;

    private final DataSource dataSource = Database.migratedDataSource();
    private final JdbcSagaStore store = new JdbcSagaStore(dataSource);
    private final List<KafkaLoop> loops = new ArrayList<>();

    @BeforeAll
    static void broker() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", KAFKA.getBootstrapServers());
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        testProducer = new KafkaProducer<>(producerProps);
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", KAFKA.getBootstrapServers());
        admin = AdminClient.create(adminProps);
    }

    @AfterAll
    static void closeBroker() {
        if (testProducer != null) {
            testProducer.close();
        }
        if (admin != null) {
            admin.close();
        }
    }

    @BeforeEach
    void cleanSlate() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM offboarding_confirmations");
            statement.executeUpdate("DELETE FROM offboarding_sagas");
        }
    }

    @AfterEach
    void stopLoops() {
        loops.forEach(KafkaLoop::shutdown);
        loops.clear();
    }

    // ------------------------------------------------------------------ (a) the happy path

    @Test
    void a_deletion_fact_becomes_a_command_and_all_confirmations_become_the_outcome() throws Exception {
        String run = run();
        String email = "happy-" + run + "@example.com";
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        String commentsTopic = "comments-events-" + run;
        createTopics(facts, memesTopic, commentsTopic,
                EventsRouter.COMMANDS_TOPIC, EventsRouter.OUTCOMES_TOPIC);
        startLoop(store, facts, Map.of(memesTopic, "memes", commentsTopic, "comments"),
                Duration.ofHours(1), new SweepOverdue(store, Duration.ofMinutes(5)));

        produce(facts, email, deletionFact(UUID.randomUUID(), email,
                ",\"policy\":{\"memes\":\"DELETE_ALL\"}"));

        JsonNode command = readMatching(EventsRouter.COMMANDS_TOPIC, Set.of(email), 1,
                GENEROUS, Duration.ZERO).getFirst();
        assertEquals("PURGE_USER_CONTENT", command.path("type").asText());
        UUID sagaId = UUID.fromString(command.path("sagaId").asText());
        assertEquals("DELETE_ALL", command.path("policy").path("memes").asText(),
                "the leaver's policy must ride the command verbatim");

        produce(memesTopic, email, confirmation(email, sagaId));
        produce(commentsTopic, email, confirmation(email, sagaId));

        JsonNode outcome = readMatching(EventsRouter.OUTCOMES_TOPIC, Set.of(email), 1,
                GENEROUS, Duration.ZERO).getFirst();
        assertEquals("PORTAL_CONTENT_PURGED", outcome.path("type").asText());
        awaitTrue("saga COMPLETED and its outcome marked announced", GENEROUS,
                () -> "COMPLETED".equals(sagaState(email)) && announced(email));
    }

    // --------------------------------------- (b) the outbox: a crash-orphaned outcome resurfaces

    @Test
    void a_completed_but_unannounced_outcome_is_republished_by_the_sweeper() throws Exception {
        String run = run();
        String email = "orphan-" + run + "@example.com";
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        // the crash in the bad window: the saga finished, the outcome never reached the broker —
        // seeded straight into the store, aged past the republish grace
        Instant past = Instant.now().minusSeconds(300);
        UUID sagaId = store.start(UUID.randomUUID(), email, past);
        store.confirm(email, sagaId, "memes", Set.of("memes"), past);
        assertEquals("COMPLETED", sagaState(email));
        assertFalse(announced(email), "the seed must model the crash BEFORE the announced mark");

        createTopics(facts, memesTopic, EventsRouter.OUTCOMES_TOPIC);
        startLoop(store, facts, Map.of(memesTopic, "memes"), Duration.ofMillis(300),
                new SweepOverdue(store, Duration.ofMinutes(5), 3, Duration.ofSeconds(1),
                        SweepOverdue.DEFAULT_RETENTION));

        JsonNode outcome = readMatching(EventsRouter.OUTCOMES_TOPIC, Set.of(email), 1,
                GENEROUS, Duration.ZERO).getFirst();
        assertEquals("PORTAL_CONTENT_PURGED", outcome.path("type").asText(),
                "the sweeper must re-announce the COMPLETED outcome, not invent another");
        awaitTrue("the announced mark earned only after the broker got the outcome", GENEROUS,
                () -> announced(email));
    }

    // ------------------------------- (c) a poison pill drops, and its offset is gone for good

    @Test
    void a_poison_pill_is_dropped_and_a_restart_never_sees_it_again() throws Exception {
        String run = run();
        String poisonEmail = "poison-" + run + "@example.com";
        String emailA = "first-" + run + "@example.com";
        String emailB = "second-" + run + "@example.com";
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        Map<String, String> participants = Map.of(memesTopic, "memes");
        createTopics(facts, memesTopic, EventsRouter.COMMANDS_TOPIC);
        KafkaLoop first = startLoop(store, facts, participants, Duration.ofHours(1),
                new SweepOverdue(store, Duration.ofMinutes(5)));

        // a fact whose id can never be a replay key, then a good one right behind it
        produce(facts, poisonEmail, "{\"id\":\"definitely-not-a-uuid\","
                + "\"type\":\"ACCOUNT_DELETION_REQUESTED\",\"email\":\"" + poisonEmail
                + "\",\"version\":1}");
        produce(facts, emailA, deletionFact(UUID.randomUUID(), emailA, ""));

        readMatching(EventsRouter.COMMANDS_TOPIC, Set.of(emailA), 1, GENEROUS, Duration.ZERO);
        assertEquals(0, sagaCount(poisonEmail), "a dropped pill must open no saga");
        // the pill's offset must be COMMITTED, not merely skipped in memory
        awaitTrue("the committed offset moves past both records", GENEROUS,
                () -> committedOffset(facts) >= 2);

        stop(first);

        // the restart: same group, same topics — replays nothing, keeps processing
        startLoop(store, facts, participants, Duration.ofHours(1),
                new SweepOverdue(store, Duration.ofMinutes(5)));
        produce(facts, emailB, deletionFact(UUID.randomUUID(), emailB, ""));
        readMatching(EventsRouter.COMMANDS_TOPIC, Set.of(emailB), 1, GENEROUS, Duration.ZERO);

        List<JsonNode> allCommands = readMatching(EventsRouter.COMMANDS_TOPIC,
                Set.of(poisonEmail, emailA, emailB), 2, GENEROUS, Duration.ofSeconds(2));
        assertEquals(2, allCommands.size(),
                "exactly one command per good fact: the restart reprocessed neither the pill nor "
                        + "the already-committed fact");
    }

    // ------------------- (b2) the outbox stays truthful when the broker REJECTS the outcome

    @Test
    void a_rejected_outcome_send_is_never_marked_announced_and_the_sweeper_delivers_after_repair()
            throws Exception {
        String run = run();
        String email = "toolarge-" + run + "@example.com";
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        createTopics(facts, memesTopic, EventsRouter.COMMANDS_TOPIC, EventsRouter.OUTCOMES_TOPIC);
        try {
            // choke the outcomes topic: max.message.bytes so small NOTHING fits, so every send
            // fails deterministically with RecordTooLargeException — the failure flush() never
            // reports and only the checked send futures can see. INSIDE the try: if the alter's
            // get() fails after the server applied it, the finally still repairs the shared topic
            outcomesTopicMaxMessageBytes("1");
            startLoop(store, facts, Map.of(memesTopic, "memes"), Duration.ofMillis(300),
                    new SweepOverdue(store, Duration.ofMinutes(5), 3, Duration.ofSeconds(1),
                            SweepOverdue.DEFAULT_RETENTION));

            produce(facts, email, deletionFact(UUID.randomUUID(), email, ""));
            JsonNode command = readMatching(EventsRouter.COMMANDS_TOPIC, Set.of(email), 1,
                    GENEROUS, Duration.ZERO).getFirst();
            UUID sagaId = UUID.fromString(command.path("sagaId").asText());
            produce(memesTopic, email, confirmation(email, sagaId));

            // the saga completes in the store, but its outcome bounces off the broker: the
            // announced mark may not appear, and no outcome may be readable on the topic
            awaitTrue("saga COMPLETED despite the choked outcomes topic", GENEROUS,
                    () -> "COMPLETED".equals(sagaState(email)));
            Thread.sleep(3_000);   // several loop passes and sweeps, all failing to deliver
            assertEquals("COMPLETED", sagaState(email));
            assertFalse(announced(email),
                    "a saga whose outcome never reached the broker must stay unannounced");
            assertEquals(0, readMatching(EventsRouter.OUTCOMES_TOPIC, Set.of(email), 0,
                            Duration.ofSeconds(2), Duration.ofSeconds(2)).size(),
                    "no outcome can have landed on a topic that rejects every record");
        } finally {
            outcomesTopicMaxMessageBytes(null);   // the repair — and cleanup for the other tests
        }

        // with the limit restored the sweeper delivers the owed outcome, and only a REAL
        // delivery earns the announced mark (the leash covers the sweeper's grown backoff)
        JsonNode outcome = readMatching(EventsRouter.OUTCOMES_TOPIC, Set.of(email), 1,
                Duration.ofSeconds(45), Duration.ZERO).getFirst();
        assertEquals("PORTAL_CONTENT_PURGED", outcome.path("type").asText());
        awaitTrue("the announced mark settles only after the repair", Duration.ofSeconds(45),
                () -> announced(email));
    }

    // ------------------- (b3) a dead broker burns no retries: the counter moves on DELIVERY only

    @Test
    void an_undeliverable_recommand_burns_no_retries_and_the_saga_never_compensates_prematurely()
            throws Exception {
        String run = run();
        String email = "deadair-" + run + "@example.com";
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        createTopics(facts, memesTopic, EventsRouter.COMMANDS_TOPIC, EventsRouter.OUTCOMES_TOPIC);
        // a saga already long overdue when the loop wakes up — the sweeper's case from sweep one
        store.start(UUID.randomUUID(), email, Instant.now().minusSeconds(600));
        try {
            // the commands topic rejects EVERY send (RecordTooLarge — the same deterministic
            // stand-in for a dead broker as the outcomes-outbox test): sweeps keep offering the
            // retry but none is ever delivered. Under the old count-on-sweep semantics,
            // maxRetries=1 would compensate on the second sweep without ONE re-command on the
            // wire; the delivered-first counter must not move at all
            topicMaxMessageBytes(EventsRouter.COMMANDS_TOPIC, "1");
            startLoop(store, facts, Map.of(memesTopic, "memes"), Duration.ofMillis(300),
                    new SweepOverdue(store, Duration.ofMinutes(5), 1, Duration.ofSeconds(1),
                            SweepOverdue.DEFAULT_RETENTION));

            Thread.sleep(4_500);   // several sweeps, every re-command bouncing off the broker
            assertEquals(0, retriesInDb(email), "an undelivered re-command must not burn a retry");
            assertEquals("STARTED", sagaState(email), "the saga must keep waiting for its retry — "
                    + "compensating with no re-command on the wire is the regression this pins");
        } finally {
            topicMaxMessageBytes(EventsRouter.COMMANDS_TOPIC, null);   // repair — and cleanup
        }

        // with the broker repaired the retry is DELIVERED, and only then counted; the silence
        // persists, so the honestly-spent counter lets a following sweep capitulate for real
        // (the long leashes cover the sweeper's grown backoff)
        readMatching(EventsRouter.COMMANDS_TOPIC, Set.of(email), 1, Duration.ofSeconds(45),
                Duration.ZERO);
        awaitTrue("the delivered retry is counted", Duration.ofSeconds(45),
                () -> retriesInDb(email) >= 1);
        awaitTrue("with the retries spent and the silence persisting, the saga compensates",
                Duration.ofSeconds(45), () -> "COMPENSATED".equals(sagaState(email)));
    }

    /** SET a max.message.bytes override on the shared outcomes topic, or DELETE it (null). */
    private static void outcomesTopicMaxMessageBytes(String value) throws Exception {
        topicMaxMessageBytes(EventsRouter.OUTCOMES_TOPIC, value);
    }

    /** SET a max.message.bytes override on a shared topic, or DELETE it (null). */
    private static void topicMaxMessageBytes(String topicName, String value) throws Exception {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        AlterConfigOp op = value == null
                ? new AlterConfigOp(new ConfigEntry("max.message.bytes", ""), AlterConfigOp.OpType.DELETE)
                : new AlterConfigOp(new ConfigEntry("max.message.bytes", value), AlterConfigOp.OpType.SET);
        admin.incrementalAlterConfigs(Map.of(topic, List.of(op))).all().get(30, TimeUnit.SECONDS);
    }

    // --------------------------- (d) an infrastructure error: rewind, back off, process once

    @Test
    void an_infrastructure_outage_is_retried_until_the_record_lands_exactly_once() throws Exception {
        String run = run();
        String email = "outage-" + run + "@example.com";
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        FailingFirstStore flaky = new FailingFirstStore(store, 2);
        createTopics(facts, memesTopic, EventsRouter.COMMANDS_TOPIC);
        KafkaLoop loop = startLoop(flaky, facts, Map.of(memesTopic, "memes"), Duration.ofHours(1),
                new SweepOverdue(flaky, Duration.ofMinutes(5)));

        produce(facts, email, deletionFact(UUID.randomUUID(), email, ""));

        // two failed passes back off 1s + 2s before the third succeeds — hence the long leash
        List<JsonNode> commands = readMatching(EventsRouter.COMMANDS_TOPIC, Set.of(email), 1,
                Duration.ofSeconds(45), Duration.ofSeconds(2));
        assertEquals(0, flaky.failuresLeft(), "the outage must actually have been exercised");
        // the guarantee is the final state, not the number of attempts: one saga, once
        assertEquals(1, sagaCount(email));
        assertEquals("STARTED", sagaState(email));
        assertEquals(1, commands.size(), "the rewound record must not double its command");
        assertTrue(loop.healthy(Duration.ofSeconds(60), Duration.ofHours(1)),
                "the loop must outlive the outage");
    }

    // ------------------------------------------------- (e) liveness: a stopped loop reports it

    @Test
    void a_stopped_loop_stalls_the_health_it_reports() throws Exception {
        String run = run();
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        createTopics(facts, memesTopic);
        KafkaLoop loop = startLoop(store, facts, Map.of(memesTopic, "memes"),
                Duration.ofMillis(200), new SweepOverdue(store, Duration.ofMinutes(5)));

        // one start per instance: a second start() would overwrite the producer and thread
        // fields under the running loops — the guard must refuse it outright
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> loop.start(KAFKA.getBootstrapServers()),
                "a second start() on the same instance must throw");

        // both loops must be genuinely PASSING, not just freshly initialised: after 3s of
        // life, the pass stamps have to be at most one poll/sweep old
        Thread.sleep(3_000);
        assertTrue(loop.healthy(Duration.ofSeconds(2), Duration.ofSeconds(2)),
                "a running loop completes passes well inside two seconds");

        stop(loop);
        Thread.sleep(1_000);
        assertFalse(loop.healthy(Duration.ofMillis(500), Duration.ofHours(1)),
                "a stopped consumer loop must read as stalled");
        assertFalse(loop.healthy(Duration.ofHours(1), Duration.ofMillis(500)),
                "a stopped sweeper loop must read as stalled");
    }

    // --------------- (f) readiness vs liveness: a permanent outage is UNREADY, never DEAD

    @Test
    void a_permanently_failing_pass_stalls_readiness_while_the_liveness_heartbeat_keeps_beating()
            throws Exception {
        String run = run();
        String email = "wedged-" + run + "@example.com";
        String facts = "security-events-" + run;
        String memesTopic = "memes-events-" + run;
        createTopics(facts, memesTopic, EventsRouter.COMMANDS_TOPIC);
        // a store that never recovers: every consumer pass over the fact fails and backs off —
        // the situation where /health must scream 503 while /alive must NOT invite a restart
        // (bouncing the process would not fix the store)
        FailingFirstStore broken = new FailingFirstStore(store, Integer.MAX_VALUE);
        KafkaLoop loop = startLoop(broken, facts, Map.of(memesTopic, "memes"),
                Duration.ofMillis(300), new SweepOverdue(broken, Duration.ofMinutes(5)));
        produce(facts, email, deletionFact(UUID.randomUUID(), email, ""));

        // the fact arrives within a poll; from then on every pass fails, backing off 1s, 2s, 4s —
        // after six seconds the last COMPLETED pass is several seconds old, the heartbeats are not
        Thread.sleep(6_000);
        assertFalse(loop.healthy(Duration.ofSeconds(2), Duration.ofHours(1)),
                "readiness: no consumer pass completes over a permanently failing store");
        assertTrue(loop.alive(Duration.ofSeconds(30)),
                "liveness: failing iterations must keep the heartbeat — the thread IS scheduling");

        stop(loop);
        assertFalse(loop.alive(Duration.ofHours(1)),
                "a dead loop thread reads dead immediately, no stall tolerance to wait out");
    }

    // ---------------------------------------------------------------------------- the wiring

    private KafkaLoop startLoop(SagaStore sagaStore, String factsTopic,
                                Map<String, String> participantByTopic, Duration sweepEvery,
                                SweepOverdue sweep) {
        Set<String> participants = Set.copyOf(participantByTopic.values());
        EventsRouter router = new EventsRouter(factsTopic, participantByTopic,
                new BeginOffboarding(sagaStore, participants),
                new RecordConfirmation(sagaStore, participants),
                sweep, MAPPER, Clock.systemUTC());
        List<String> topics = new ArrayList<>(participantByTopic.keySet());
        topics.add(factsTopic);
        KafkaLoop loop = new KafkaLoop(router, sagaStore, topics, sweepEvery);
        loop.start(KAFKA.getBootstrapServers());
        loops.add(loop);
        return loop;
    }

    private void stop(KafkaLoop loop) {
        loop.shutdown();
        loops.remove(loop);
    }

    /** A short unique tag: per-test topics and emails, so the shared topics stay readable. */
    private static String run() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static void createTopics(String... names) throws Exception {
        List<NewTopic> topics = new ArrayList<>();
        for (String name : names) {
            topics.add(new NewTopic(name, 1, (short) 1));
        }
        for (Map.Entry<String, KafkaFuture<Void>> creation
                : admin.createTopics(topics).values().entrySet()) {
            try {
                creation.getValue().get(30, TimeUnit.SECONDS);
            } catch (ExecutionException existsMaybe) {
                if (!(existsMaybe.getCause() instanceof TopicExistsException)) {
                    throw existsMaybe;
                }
            }
        }
    }

    private static void produce(String topic, String key, String value) throws Exception {
        testProducer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
    }

    private static String deletionFact(UUID factId, String email, String extraJson) {
        return "{\"id\":\"" + factId + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\",\"email\":\""
                + email + "\",\"version\":1" + extraJson + "}";
    }

    private static String confirmation(String email, UUID sagaId) {
        return "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"USER_CONTENT_PURGED\",\"email\":\""
                + email + "\",\"sagaId\":\"" + sagaId + "\",\"version\":1}";
    }

    /**
     * Read a topic from the beginning with a throwaway group and keep the records whose email is
     * one of ours (the command/outcome topics are shared across tests). Waits until {@code atLeast}
     * matches arrived; a non-zero {@code settle} then keeps listening for stragglers — that is how
     * the exact-count assertions catch a duplicate that should not exist.
     */
    private static List<JsonNode> readMatching(String topic, Set<String> emails, int atLeast,
                                               Duration timeout, Duration settle) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("group.id", "probe-" + UUID.randomUUID());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        List<JsonNode> matches = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        try (KafkaConsumer<String, String> probe = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = probe.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            probe.assign(partitions);
            probe.seekToBeginning(partitions);
            long settledAt = Long.MAX_VALUE;
            while (System.currentTimeMillis() < deadline
                    && System.currentTimeMillis() < settledAt) {
                for (ConsumerRecord<String, String> record : probe.poll(Duration.ofMillis(200))) {
                    JsonNode event = MAPPER.readTree(record.value());
                    if (emails.contains(event.path("email").asText())) {
                        matches.add(event);
                        settledAt = Long.MAX_VALUE;   // a new match restarts the settle window
                    }
                }
                if (matches.size() >= atLeast && settledAt == Long.MAX_VALUE) {
                    if (settle.isZero()) {
                        return matches;
                    }
                    settledAt = System.currentTimeMillis() + settle.toMillis();
                }
            }
        }
        if (matches.size() < atLeast) {
            fail("expected at least " + atLeast + " record(s) for " + emails + " on " + topic
                    + " within " + timeout + " but saw " + matches.size() + ": " + matches);
        }
        return matches;
    }

    /** The loop group's committed offset on the (single-partition) topic; -1 while none. */
    private static long committedOffset(String topic) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(LOOP_GROUP)
                    .partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);
            OffsetAndMetadata offset = committed.get(new TopicPartition(topic, 0));
            return offset == null ? -1 : offset.offset();
        } catch (Exception unavailable) {
            return -1;
        }
    }

    private static void awaitTrue(String what, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("gave up waiting for: " + what);
    }

    // ------------------------------------------------------------------ the store's state, raw

    private String sagaState(String email) {
        return querySaga(email, "state").orElse(null);
    }

    private boolean announced(String email) {
        return querySaga(email, "outcome_announced").map(Boolean::parseBoolean).orElse(false);
    }

    private int retriesInDb(String email) {
        return querySaga(email, "retries").map(Integer::parseInt).orElse(-1);
    }

    private int sagaCount(String email) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM offboarding_sagas WHERE email = ?")) {
            statement.setString(1, email);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Optional<String> querySaga(String email, String column) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT " + column + " FROM offboarding_sagas WHERE email = ?")) {
            statement.setString(1, email);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.ofNullable(result.getString(1)) : Optional.empty();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The simulated infrastructure outage: the first {@code failures} calls to {@link #start}
     * throw — the loop must rewind, back off and retry until the store recovers, and the
     * idempotent transitions must keep the retries invisible in the final state.
     */
    private static final class FailingFirstStore implements SagaStore {
        private final SagaStore inner;
        private final AtomicInteger failures;

        FailingFirstStore(SagaStore inner, int failures) {
            this.inner = inner;
            this.failures = new AtomicInteger(failures);
        }

        int failuresLeft() {
            return Math.max(0, failures.get());
        }

        @Override
        public UUID start(UUID factId, String email, String policy, Instant at) {
            if (failures.get() > 0) {
                failures.decrementAndGet();
                throw new IllegalStateException("simulated database outage");
            }
            return inner.start(factId, email, policy, at);
        }

        @Override
        public java.util.Optional<UUID> confirm(String email, UUID sagaId, String participant,
                                                Set<String> required, Instant at) {
            return inner.confirm(email, sagaId, participant, required, at);
        }

        @Override
        public boolean complete(String email, Instant at) {
            return inner.complete(email, at);
        }

        @Override
        public SweepResult sweepOverdue(Instant cutoff, int maxRetries, Instant at) {
            return inner.sweepOverdue(cutoff, maxRetries, at);
        }

        @Override
        public void retryDelivered(UUID sagaId) {
            inner.retryDelivered(sagaId);
        }

        @Override
        public void markAnnounced(UUID sagaId) {
            inner.markAnnounced(sagaId);
        }

        @Override
        public List<PendingOutcome> unannouncedOutcomes(Instant olderThan) {
            return inner.unannouncedOutcomes(olderThan);
        }

        @Override
        public int deleteFinishedBefore(Instant olderThan) {
            return inner.deleteFinishedBefore(olderThan);
        }
    }
}
