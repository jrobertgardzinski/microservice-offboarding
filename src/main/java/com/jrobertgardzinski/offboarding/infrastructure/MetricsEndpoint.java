package com.jrobertgardzinski.offboarding.infrastructure;

import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The service's vitals in Prometheus text format at {@code /metrics}, scraped by the workspace's
 * Prometheus (job "offboarding"). Hand-rolled to the house's lean taste, matching the manual
 * exporters in the formula backend and user-collections — the JVM's basics, uptime, and the
 * business numbers worth watching: how many sagas the sweeper gave up on (the alert), how many
 * re-commands demonstrably reached the broker (the saga is limping but fighting), and how many
 * sweeper passes failed outright (infrastructure trouble on the timeout path).
 */
final class MetricsEndpoint {

    private static final long STARTED = System.currentTimeMillis();
    private static final AtomicLong COMPENSATED = new AtomicLong();
    private static final AtomicLong RETRIES_DELIVERED = new AtomicLong();
    private static final AtomicLong SWEEPER_PASS_FAILURES = new AtomicLong();

    private MetricsEndpoint() {
    }

    /** The sweeper capitulated on this many sagas; the router reports each batch it announces. */
    static void compensated(int count) {
        COMPENSATED.addAndGet(count);
    }

    /** One re-commanded purge demonstrably reached the broker (the delivered-first counter). */
    static void retryDelivered() {
        RETRIES_DELIVERED.incrementAndGet();
    }

    /** One sweeper pass died on infrastructure and will be retried after the backoff. */
    static void sweeperPassFailed() {
        SWEEPER_PASS_FAILURES.incrementAndGet();
    }

    static void handle(ServerRequest req, ServerResponse res) {
        res.send(body());
    }

    /** The exposition text — separate from the HTTP plumbing so the test can read the counters. */
    static String body() {
        Runtime rt = Runtime.getRuntime();
        return "# TYPE offboarding_jvm_memory_used_bytes gauge\n"
                + "offboarding_jvm_memory_used_bytes " + (rt.totalMemory() - rt.freeMemory()) + "\n"
                + "# TYPE offboarding_jvm_threads gauge\n"
                + "offboarding_jvm_threads " + ManagementFactory.getThreadMXBean().getThreadCount() + "\n"
                + "# TYPE offboarding_uptime_seconds gauge\n"
                + "offboarding_uptime_seconds " + (System.currentTimeMillis() - STARTED) / 1000 + "\n"
                + "# TYPE offboarding_sagas_compensated_total counter\n"
                + "offboarding_sagas_compensated_total " + COMPENSATED.get() + "\n"
                + "# TYPE offboarding_retries_delivered_total counter\n"
                + "offboarding_retries_delivered_total " + RETRIES_DELIVERED.get() + "\n"
                + "# TYPE offboarding_sweeper_pass_failures_total counter\n"
                + "offboarding_sweeper_pass_failures_total " + SWEEPER_PASS_FAILURES.get() + "\n";
    }
}
