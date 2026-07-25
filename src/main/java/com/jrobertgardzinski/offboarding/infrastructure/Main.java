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
 * {@code OFFBOARDING_SWEEPER_STALL_SEC}). Mind what that buys: compose does NOT restart an
 * unhealthy container — its healthcheck only makes the wedge VISIBLE ({@code docker ps}, the
 * e2e preflight) and gates dependants via {@code depends_on: condition: service_healthy}. An
 * actual automatic restart takes an orchestrator with a liveness probe wired to /health — the
 * k3s deployment (HOSTING-K3S.md) is where a wedged loop gets bounced without a human.
 */
public final class Main {

    static final String DEFAULT_PARTICIPANTS =
            "memes=memes-events,comments=comments-events,collections=usercollections-events";

    /** How often the sweeper wakes ({@link KafkaLoop} sweep interval) — also the floor for the
     *  sweeper's stall tolerance: liveness is stamped at most once per interval, so a smaller
     *  tolerance would flag a perfectly healthy sweeper as stalled. */
    static final Duration SWEEP_EVERY = Duration.ofSeconds(15);

    private Main() {
    }

    public static void main(String[] args) {
        int port = (int) longEnv("OFFBOARDING_PORT", 8094);
        String factsTopic = System.getenv().getOrDefault("OFFBOARDING_FACTS_TOPIC", "security-events");
        Map<String, String> participantByTopic = parseParticipants(
                System.getenv().getOrDefault("OFFBOARDING_PARTICIPANTS", DEFAULT_PARTICIPANTS));
        Duration purgeTimeout = Duration.ofSeconds(longEnv("OFFBOARDING_PURGE_TIMEOUT_SEC", 120));
        int maxPurgeRetries = (int) longEnv("OFFBOARDING_MAX_PURGE_RETRIES",
                SweepOverdue.DEFAULT_MAX_RETRIES);
        Duration republishAfter = Duration.ofSeconds(longEnv("OFFBOARDING_OUTCOME_REPUBLISH_SEC",
                SweepOverdue.DEFAULT_REPUBLISH_AFTER.toSeconds()));
        Duration retention = Duration.ofDays(longEnv("OFFBOARDING_RETENTION_DAYS",
                SweepOverdue.DEFAULT_RETENTION.toDays()));
        Duration consumerStall = Duration.ofSeconds(longEnv("OFFBOARDING_CONSUMER_STALL_SEC", 60));
        Duration sweeperStall = flooredSweeperStall(
                Duration.ofSeconds(longEnv("OFFBOARDING_SWEEPER_STALL_SEC", 60)), SWEEP_EVERY);

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
            kafkaLoop = new KafkaLoop(router, store, topics, SWEEP_EVERY);
            kafkaLoop.start(bootstrap);
        }
        KafkaLoop loop = kafkaLoop;

        WebServer server = WebServer.builder()
                .port(port)
                .routing(routing -> routing
                        .get("/health", (req, res) -> {
                            // the healthcheck watches this: a stalled loop turns the container
                            // unhealthy — under compose that only makes the wedge visible and
                            // gates depends_on; a restart takes an orchestrator with a liveness
                            // probe (see the class javadoc)
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

    /**
     * A numeric env var, or the default when absent/blank. A mangled value refuses to boot with a
     * message that NAMES the variable — a bare NumberFormatException("For input string: \"abc\"")
     * names neither the variable nor the fix, and this service boots from a dozen of these.
     */
    static long longEnv(String name, long defaultValue) {
        return parseLongOrRefuse(name, System.getenv(name), defaultValue);
    }

    static long parseLongOrRefuse(String name, String raw, long defaultValue) {
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException mangled) {
            throw new IllegalArgumentException(
                    name + " must be a whole number, got \"" + raw + "\"");
        }
    }

    /**
     * The sweeper stamps liveness once per {@code sweepEvery} at best, so a stall tolerance below
     * the interval would report a healthy sweeper as stalled on every check. Floor it, loudly.
     */
    static Duration flooredSweeperStall(Duration configured, Duration sweepEvery) {
        if (configured.compareTo(sweepEvery) >= 0) {
            return configured;
        }
        System.err.println("OFFBOARDING_SWEEPER_STALL_SEC=" + configured.toSeconds()
                + " is below the sweep interval of " + sweepEvery.toSeconds() + "s — the sweeper"
                + " can only stamp liveness once per interval, so /health would call a healthy"
                + " sweeper stalled; using " + sweepEvery.toSeconds() + "s instead");
        return sweepEvery;
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
