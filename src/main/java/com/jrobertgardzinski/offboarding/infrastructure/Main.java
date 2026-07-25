package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SagaStore;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import io.helidon.webserver.WebServer;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Boots the portal's offboarding orchestrator: the participants come from CONFIGURATION
 * ({@code OFFBOARDING_PARTICIPANTS}, {@code name=confirmation-topic} pairs), never from code —
 * the whole point of extracting this saga out of microservice-security. Port comes from
 * {@code OFFBOARDING_PORT} (default 8094 — next free after collections-ui's 8093); HTTP serves
 * only {@code /health} and {@code /metrics}, the saga itself lives on Kafka.
 *
 * <p>Saga state is Postgres when {@code DB_URL} is set, else in-memory H2. Without
 * {@code KAFKA_BOOTSTRAP_SERVERS} the loop simply never runs (dev, tests) — and then /health has
 * no loops to distrust. With Kafka, /health turns 503 once either loop stops completing passes
 * for longer than its stall tolerance ({@code OFFBOARDING_CONSUMER_STALL_SEC} /
 * {@code OFFBOARDING_SWEEPER_STALL_SEC}), which is what lets the compose healthcheck restart a
 * wedged container instead of admiring it.
 */
public final class Main {

    static final String DEFAULT_PARTICIPANTS =
            "memes=memes-events,comments=comments-events,collections=usercollections-events";

    private Main() {
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("OFFBOARDING_PORT", "8094"));
        String factsTopic = System.getenv().getOrDefault("OFFBOARDING_FACTS_TOPIC", "security-events");
        Map<String, String> participantByTopic = parseParticipants(
                System.getenv().getOrDefault("OFFBOARDING_PARTICIPANTS", DEFAULT_PARTICIPANTS));
        Duration purgeTimeout = Duration.ofSeconds(Long.parseLong(
                System.getenv().getOrDefault("OFFBOARDING_PURGE_TIMEOUT_SEC", "120")));
        int maxPurgeRetries = Integer.parseInt(System.getenv().getOrDefault(
                "OFFBOARDING_MAX_PURGE_RETRIES", String.valueOf(SweepOverdue.DEFAULT_MAX_RETRIES)));
        Duration republishAfter = Duration.ofSeconds(Long.parseLong(System.getenv().getOrDefault(
                "OFFBOARDING_OUTCOME_REPUBLISH_SEC",
                String.valueOf(SweepOverdue.DEFAULT_REPUBLISH_AFTER.toSeconds()))));
        Duration retention = Duration.ofDays(Long.parseLong(System.getenv().getOrDefault(
                "OFFBOARDING_RETENTION_DAYS", String.valueOf(SweepOverdue.DEFAULT_RETENTION.toDays()))));
        Duration consumerStall = Duration.ofSeconds(Long.parseLong(
                System.getenv().getOrDefault("OFFBOARDING_CONSUMER_STALL_SEC", "60")));
        Duration sweeperStall = Duration.ofSeconds(Long.parseLong(
                System.getenv().getOrDefault("OFFBOARDING_SWEEPER_STALL_SEC", "60")));

        DataSource dataSource = Database.migratedDataSource();
        SagaStore store = new JdbcSagaStore(dataSource);
        var participants = Map.copyOf(participantByTopic).values().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        EventsRouter router = new EventsRouter(factsTopic, participantByTopic,
                new BeginOffboarding(store, participants),
                new RecordConfirmation(store, participants),
                new SweepOverdue(store, purgeTimeout, maxPurgeRetries, republishAfter, retention),
                new ObjectMapper(), Clock.systemUTC());

        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "").trim();
        KafkaLoop kafkaLoop = null;
        if (!bootstrap.isEmpty()) {
            List<String> topics = new ArrayList<>(participantByTopic.keySet());
            topics.add(factsTopic);
            kafkaLoop = new KafkaLoop(router, store, topics, Duration.ofSeconds(15));
            kafkaLoop.start(bootstrap);
        }
        KafkaLoop loop = kafkaLoop;

        WebServer server = WebServer.builder()
                .port(port)
                .routing(routing -> routing
                        .get("/health", (req, res) -> {
                            // the compose healthcheck watches this: a stalled loop turns the
                            // container unhealthy so the orchestrator restarts it
                            if (loop == null || loop.healthy(consumerStall, sweeperStall)) {
                                res.send("OK");
                            } else {
                                res.status(503).send("loop stalled");
                            }
                        })
                        .get("/metrics", MetricsEndpoint::handle))
                .build()
                .start();

        System.out.println("offboarding listening on port " + server.port()
                + " (participants: " + participants + ")");
    }

    /** {@code memes=memes-events,comments=comments-events} → {topic → participant}. */
    static Map<String, String> parseParticipants(String spec) {
        Map<String, String> byTopic = new LinkedHashMap<>();
        for (String pair : spec.split(",")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                continue;   // an empty spec means: no content participants at all
            }
            String[] parts = trimmed.split("=", 2);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException("participant entry must be name=topic: " + trimmed);
            }
            byTopic.put(parts[1].trim(), parts[0].trim());
        }
        return byTopic;
    }
}
