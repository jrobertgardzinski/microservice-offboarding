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
 * scheduled for longer than {@code OFFBOARDING_ALIVE_STALL_SEC} (default 240s; floored at
 * {@link #ALIVE_STALL_FLOOR}, the SUM of every block one iteration can spend — the rewind lookup,
 * the readiness probe, the poll, the DATABASE, the flush, the commit and the retry backoff —
 * plus a margin) — THAT is what an orchestrator's liveness
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

    /**
     * The worst LEGAL consumer iteration, block by block — the sum of every clock one pass can
     * spend, in the order {@link KafkaLoop#consume} spends them. Written out as a sum on purpose:
     * the floor used to be {@code 2 x max(...)} of the same clocks, a shape that LOOKS
     * conservative and is not. Two of the longest block (30s) plus a sweep and a probe came to
     * 80s, while an honest worst iteration adds up to 146s — so the "safe minimum" sat well below
     * the case it was sold as covering, and a broker outage on a slow database could still read
     * as a dead thread. Every term below is one real block:
     *
     * <ul>
     *   <li>{@link KafkaLoop#API_TIMEOUT} — the rewind's {@code committed()} lookup, when the
     *       previous pass failed</li>
     *   <li>{@link KafkaLoop#PROBE_TIMEOUT} — the /health honesty probe</li>
     *   <li>{@link KafkaLoop#POLL_TIMEOUT} — the poll itself</li>
     *   <li>{@link Database#WORST_BLOCK} — the router's saga reads and writes; ONE such block,
     *       because the first database failure throws out of the pass and the store calls behind
     *       it never run</li>
     *   <li>{@link KafkaLoop#DELIVERY_TIMEOUT} — {@code flush()} (= {@link KafkaLoop#MAX_BLOCK}
     *       for a send still waiting on metadata); {@code settleDeliveries} adds nothing, the
     *       futures are settled by the time it looks at them</li>
     *   <li>{@link KafkaLoop#API_TIMEOUT} again — {@code commitSync()}</li>
     *   <li>{@link KafkaLoop#MAX_BACKOFF} — the pause before the retry, paid INSIDE the iteration
     *       that failed</li>
     * </ul>
     */
    static final Duration CONSUMER_WORST_ITERATION = KafkaLoop.API_TIMEOUT
            .plus(KafkaLoop.PROBE_TIMEOUT)
            .plus(KafkaLoop.POLL_TIMEOUT)
            .plus(Database.WORST_BLOCK)
            .plus(KafkaLoop.DELIVERY_TIMEOUT)
            .plus(KafkaLoop.API_TIMEOUT)
            .plus(KafkaLoop.MAX_BACKOFF);

    /**
     * The same accounting for the OTHER loop: the sweeper sleeps its whole interval, reads and
     * writes the store, flushes what it announced, and backs off if that failed. It has no poll,
     * no probe and no commit — which is why the consumer, not the sweeper, sets the floor. Both
     * threads share ONE tolerance ({@code alive()} demands a fresh beat from each), so the floor
     * has to cover whichever is worse.
     */
    static final Duration SWEEPER_WORST_ITERATION = SWEEP_EVERY
            .plus(Database.WORST_BLOCK)
            .plus(KafkaLoop.DELIVERY_TIMEOUT)
            .plus(KafkaLoop.MAX_BACKOFF);

    /** The /alive stall tolerance's floor, DERIVED from the loops' own clocks (never a magic
     *  number): the longer of the two worst iterations above, plus
     *  {@link #FLOOR_MARGIN_PERCENT}. Below it a mere broker or database outage — every clock of
     *  which is legal and bounded — would read as a dead thread, restarting a pod a restart
     *  cannot fix. Currently 183s (consumer 146s + 25%); the 240s default sits above it. */
    static final Duration ALIVE_STALL_FLOOR =
            withMargin(max(CONSUMER_WORST_ITERATION, SWEEPER_WORST_ITERATION));

    /** The code default for {@code OFFBOARDING_ALIVE_STALL_SEC}. A named constant, not a literal
     *  in {@code main()}, so the test can assert the one property a default must have: that it
     *  sits ABOVE {@link #ALIVE_STALL_FLOOR}. The previous 120s did not, once the floor was
     *  computed honestly — and a default that the floor silently corrects is a lie in the
     *  javadoc, the manifests and the operator's head at once. */
    static final Duration DEFAULT_ALIVE_STALL = Duration.ofSeconds(240);

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
        // 240s default: it has to sit ABOVE ALIVE_STALL_FLOOR, and the floor is now the honest
        // SUM of one iteration's blocks (rewind lookup 20s + probe 5s + poll 1s + database 40s +
        // flush 30s + commit 20s + backoff 30s = 146s, plus 25% margin = 183s) rather than the
        // 2 x max(...) shape that used to under-count it at 100s. The old 120s default was BELOW
        // that honest floor, so every boot would have been silently floored — a default that
        // needs correcting is not a default
        Duration aliveStall = flooredAliveStall("OFFBOARDING_ALIVE_STALL_SEC",
                Duration.ofSeconds(longEnv("OFFBOARDING_ALIVE_STALL_SEC",
                        DEFAULT_ALIVE_STALL.toSeconds(), 1, Long.MAX_VALUE)));

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
                            // process would not fix the broker (that is /alive's job).
                            // The tolerance is not a detection deadline: the broker probe runs
                            // on a cadence (KafkaLoop.PROBE_EVERY, 10s), so a broker dying just
                            // after a successful probe is noticed up to a cadence plus a probe
                            // timeout later than the configured stall — see healthy()'s javadoc
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
     * The /alive tolerance's own floor, {@link #ALIVE_STALL_FLOOR}: one iteration can legally
     * spend EVERY block in {@link #CONSUMER_WORST_ITERATION} back to back — they are alternatives
     * only in the happy case, and a broker outage arriving on top of a slow database pays them in
     * sequence — so a tolerance below their sum (plus a margin) would let an outage read as a
     * dead thread, the exact restart-loop /alive exists to prevent. The message spells the sum
     * out term by term: an operator who is told "below the floor" deserves to see WHICH clocks
     * add up to it.
     */
    static Duration flooredAliveStall(String name, Duration configured) {
        if (configured.compareTo(ALIVE_STALL_FLOOR) >= 0) {
            return configured;
        }
        LOG.warn("{}={}s is below the {}s floor, the SUM of one iteration's blocks (rewind"
                        + " lookup {}s + broker probe {}s + poll {}s + database {}s + flush {}s"
                        + " + commit {}s + max backoff {}s = {}s for the consumer, {}s for the"
                        + " sweeper, plus {}% margin) — an outage legitimately holds an iteration"
                        + " that long, and a smaller tolerance would let /alive restart a pod"
                        + " over a broker or database problem; using {}s instead",
                name, configured.toSeconds(), ALIVE_STALL_FLOOR.toSeconds(),
                KafkaLoop.API_TIMEOUT.toSeconds(), KafkaLoop.PROBE_TIMEOUT.toSeconds(),
                KafkaLoop.POLL_TIMEOUT.toSeconds(), Database.WORST_BLOCK.toSeconds(),
                KafkaLoop.DELIVERY_TIMEOUT.toSeconds(), KafkaLoop.API_TIMEOUT.toSeconds(),
                KafkaLoop.MAX_BACKOFF.toSeconds(), CONSUMER_WORST_ITERATION.toSeconds(),
                SWEEPER_WORST_ITERATION.toSeconds(), FLOOR_MARGIN_PERCENT,
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
