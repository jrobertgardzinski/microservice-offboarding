package com.jrobertgardzinski.offboarding.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jrobertgardzinski.offboarding.application.BeginOffboarding;
import com.jrobertgardzinski.offboarding.application.RecordConfirmation;
import com.jrobertgardzinski.offboarding.application.SagaStore;
import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import io.helidon.webserver.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * only {@code /health}, {@code /alive} and {@code /metrics}, the saga itself lives on Kafka.
 *
 * <p>Saga state is Postgres when {@code DB_URL} is set, else in-memory H2. Without
 * {@code KAFKA_BOOTSTRAP_SERVERS} the loop simply never runs (dev, tests) — and then the probes
 * have no loops to distrust. With Kafka the two probes split readiness from liveness:
 * {@code /health} (READINESS) turns 503 once either loop stops COMPLETING passes for longer than
 * its stall tolerance ({@code OFFBOARDING_CONSUMER_STALL_SEC} /
 * {@code OFFBOARDING_SWEEPER_STALL_SEC}) — a broker outage does that, and restarting would not
 * fix the broker, so compose healthchecks and {@code depends_on} gate on it while dependants
 * wait. {@code /alive} (LIVENESS) turns 503 only when a loop thread died or stopped being
 * scheduled for longer than {@code OFFBOARDING_ALIVE_STALL_SEC} (default 120s; floored at
 * {@link #ALIVE_STALL_FLOOR}, derived from the producer's delivery timeout, the CONSUMER's api
 * timeout, the readiness probe and the retry backoff) — THAT is what an orchestrator's liveness
 * probe restarts; the k3s deployment
 * (HOSTING-K3S.md) is where a genuinely dead loop gets bounced without a human.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    static final String DEFAULT_PARTICIPANTS =
            "memes=memes-events,comments=comments-events,collections=usercollections-events";

    /** How often the sweeper wakes ({@link KafkaLoop} sweep interval) — also the floor for the
     *  sweeper's stall tolerance: liveness is stamped at most once per interval, so a smaller
     *  tolerance would flag a perfectly healthy sweeper as stalled. */
    static final Duration SWEEP_EVERY = Duration.ofSeconds(15);

    /** The safety margin the derived floor carries on top of the arithmetic: {@code alive()}
     *  compares the age of the beat with {@code <=}, and an iteration that legitimately spends
     *  every clock it is allowed still pays for a GC pause, a socket settling or the scheduler's
     *  own latency on top — an exactly-tight floor would call that a dead thread. */
    static final int FLOOR_MARGIN_PERCENT = 25;

    /** The /alive stall tolerance's floor, DERIVED from the loop's own clocks (never a magic
     *  number): during a broker outage one iteration legitimately holds a send/flush for up to
     *  {@link KafkaLoop#DELIVERY_TIMEOUT} (= {@link KafkaLoop#MAX_BLOCK}), a commit or a rewind
     *  lookup for up to {@link KafkaLoop#API_TIMEOUT}, the readiness probe for up to
     *  {@link KafkaLoop#PROBE_TIMEOUT}, and then backs off up to {@link KafkaLoop#MAX_BACKOFF} —
     *  so the tolerance covers two of the longest of those plus one sweep interval and one probe,
     *  plus {@link #FLOOR_MARGIN_PERCENT}. Below that a mere broker outage could outlast the
     *  probe and read as a dead thread, restarting a pod a restart cannot fix. Currently 100s;
     *  the 120s default sits above it. */
    static final Duration ALIVE_STALL_FLOOR = withMargin(
            max(max(KafkaLoop.DELIVERY_TIMEOUT, KafkaLoop.API_TIMEOUT), KafkaLoop.MAX_BACKOFF)
                    .multipliedBy(2)
                    .plus(SWEEP_EVERY)
                    .plus(KafkaLoop.PROBE_TIMEOUT));

    private static Duration max(Duration a, Duration b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    /** The derived worst case plus {@link #FLOOR_MARGIN_PERCENT}, rounded UP to whole seconds —
     *  the tolerance is configured and logged in seconds, so the floor lives in them too. */
    private static Duration withMargin(Duration derived) {
        long millis = derived.toMillis() * (100 + FLOOR_MARGIN_PERCENT) / 100;
        return Duration.ofSeconds(Math.ceilDiv(millis, 1_000));
    }

    private Main() {
    }

    public static void main(String[] args) {
        // every numeric env is range-checked at boot: a port outside 1-65535, a negative retry
        // budget or a non-positive timeout cannot mean anything the operator intended, and
        // refusing with the variable's name and value beats booting into quiet nonsense. The
        // (int) casts sit on checked ranges, so they can no longer truncate silently
        int port = (int) longEnv("OFFBOARDING_PORT", 8094, 1, 65535);
        String factsTopic = System.getenv().getOrDefault("OFFBOARDING_FACTS_TOPIC", "security-events");
        Map<String, String> participantByTopic = parseParticipants(
                System.getenv().getOrDefault("OFFBOARDING_PARTICIPANTS", DEFAULT_PARTICIPANTS));
        Duration purgeTimeout = Duration.ofSeconds(
                longEnv("OFFBOARDING_PURGE_TIMEOUT_SEC", 120, 1, Long.MAX_VALUE));
        int maxPurgeRetries = (int) longEnv("OFFBOARDING_MAX_PURGE_RETRIES",
                SweepOverdue.DEFAULT_MAX_RETRIES, 0, 100);
        Duration republishAfter = Duration.ofSeconds(longEnv("OFFBOARDING_OUTCOME_REPUBLISH_SEC",
                SweepOverdue.DEFAULT_REPUBLISH_AFTER.toSeconds(), 1, Long.MAX_VALUE));
        Duration retention = Duration.ofDays(longEnv("OFFBOARDING_RETENTION_DAYS",
                SweepOverdue.DEFAULT_RETENTION.toDays(), 1, Long.MAX_VALUE));
        Duration consumerStall = Duration.ofSeconds(
                longEnv("OFFBOARDING_CONSUMER_STALL_SEC", 60, 1, Long.MAX_VALUE));
        Duration sweeperStall = flooredStall("OFFBOARDING_SWEEPER_STALL_SEC",
                Duration.ofSeconds(longEnv("OFFBOARDING_SWEEPER_STALL_SEC", 60, 1, Long.MAX_VALUE)),
                SWEEP_EVERY);
        // 120s default: covers the worst legitimate iteration gap during a broker outage —
        // sweep interval (15s) + a send/flush blocked up to delivery.timeout (30s) or a commit /
        // rewind lookup blocked up to default.api.timeout (20s) + the readiness probe (5s) + one
        // max backoff (30s) — with room to spare; the floor below is derived from those same
        // constants (plus a margin) so a smaller configured value cannot turn an outage into a
        // fake 503
        Duration aliveStall = flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Duration.ofSeconds(longEnv("OFFBOARDING_ALIVE_STALL_SEC", 120, 1, Long.MAX_VALUE)));

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
                            // READINESS: passes must COMPLETE. A broker or database outage turns
                            // this 503 so compose healthchecks and depends_on gate on it — but an
                            // orchestrator must NOT use it as a liveness probe: restarting the
                            // process would not fix the broker (that is /alive's job)
                            if (loop == null || loop.healthy(consumerStall, sweeperStall)) {
                                res.send("OK");
                            } else {
                                res.status(503).send("loop stalled");
                            }
                        })
                        .get("/alive", (req, res) -> {
                            // LIVENESS: the loop threads must live and keep being scheduled —
                            // iterations count, not successes, so an outage mid-backoff stays
                            // 200 here (and 503 on /health above). Only a genuinely dead or
                            // wedged thread turns this 503, for the orchestrator's liveness
                            // probe (k3s, HOSTING-K3S.md) to bounce the process
                            if (loop == null || loop.alive(aliveStall)) {
                                res.send("OK");
                            } else {
                                res.status(503).send("loop thread dead");
                            }
                        })
                        .get("/metrics", MetricsEndpoint::handle))
                .build()
                .start();

        System.out.println("offboarding listening on port " + server.port()
                + " (participants: " + participants + ")");
    }

    /**
     * A numeric env var, or the default when absent/blank — range-checked, because every one of
     * these has values that cannot mean anything (a negative timeout, port 0). A mangled or
     * out-of-range value refuses to boot with a message that NAMES the variable and echoes the
     * value — a bare NumberFormatException("For input string: \"abc\"") names neither the
     * variable nor the fix, and this service boots from a dozen of these.
     */
    static long longEnv(String name, long defaultValue, long min, long max) {
        return inRangeOrRefuse(name, parseLongOrRefuse(name, System.getenv(name), defaultValue),
                min, max);
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

    static long inRangeOrRefuse(String name, long value, long min, long max) {
        if (value >= min && value <= max) {
            return value;
        }
        throw new IllegalArgumentException(max == Long.MAX_VALUE
                ? name + " must be at least " + min + ", got " + value
                : name + " must be between " + min + " and " + max + ", got " + value);
    }

    /**
     * The loops stamp their markers once per {@code sweepEvery} at best (the sweeper sleeps the
     * whole interval between stamps), so a stall tolerance below the interval would report a
     * perfectly healthy loop as stalled on every check. Floor it — loudly, through the logger,
     * where the service's own WARNs live (System.err bypasses the log shipping).
     */
    static Duration flooredStall(String name, Duration configured, Duration sweepEvery) {
        if (configured.compareTo(sweepEvery) >= 0) {
            return configured;
        }
        LOG.warn("{}={} is below the sweep interval of {}s — the sweeper can only stamp its"
                        + " marker once per interval, so the probe would call a healthy loop"
                        + " stalled; using {}s instead", name, configured.toSeconds(),
                sweepEvery.toSeconds(), sweepEvery.toSeconds());
        return sweepEvery;
    }

    /**
     * The /alive tolerance's own floor, {@link #ALIVE_STALL_FLOOR}: during a broker outage an
     * iteration legitimately blocks for up to the producer's delivery timeout OR one of the
     * consumer's API waits (commit, rewind lookup, readiness probe) and then backs off, so a
     * tolerance below twice the longest of those clocks (plus a sweep interval, a probe and a
     * margin) would let a mere outage read as a dead thread — the exact restart-loop /alive
     * exists to prevent.
     */
    static Duration flooredAliveStall(String name, Duration configured) {
        if (configured.compareTo(ALIVE_STALL_FLOOR) >= 0) {
            return configured;
        }
        LOG.warn("{}={}s is below the {}s floor derived from the loop's clocks (2 x max of"
                        + " delivery.timeout {}s, default.api.timeout {}s and max backoff {}s,"
                        + " plus the {}s sweep interval and the {}s broker probe, plus {}%"
                        + " margin) — a broker outage legitimately holds an iteration that"
                        + " long, and a smaller tolerance would let /alive restart a pod over a"
                        + " broker problem; using {}s instead",
                name, configured.toSeconds(), ALIVE_STALL_FLOOR.toSeconds(),
                KafkaLoop.DELIVERY_TIMEOUT.toSeconds(), KafkaLoop.API_TIMEOUT.toSeconds(),
                KafkaLoop.MAX_BACKOFF.toSeconds(), SWEEP_EVERY.toSeconds(),
                KafkaLoop.PROBE_TIMEOUT.toSeconds(), FLOOR_MARGIN_PERCENT,
                ALIVE_STALL_FLOOR.toSeconds());
        return ALIVE_STALL_FLOOR;
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
