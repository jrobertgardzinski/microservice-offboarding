package com.jrobertgardzinski.offboarding.infrastructure;

import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The service's vitals in Prometheus text format at {@code /metrics}, scraped by the workspace's
 * Prometheus (job "offboarding"). Hand-rolled to the house's lean taste, matching the manual
 * exporters in the formula backend and user-collections — the JVM's basics, uptime, and the one
 * business number worth alerting on: how many sagas the sweeper gave up on.
 */
final class MetricsEndpoint {

    private static final long STARTED = System.currentTimeMillis();
    private static final AtomicLong COMPENSATED = new AtomicLong();

    private MetricsEndpoint() {
    }

    /** The sweeper capitulated on this many sagas; the router reports each batch it announces. */
    static void compensated(int count) {
        COMPENSATED.addAndGet(count);
    }

    static void handle(ServerRequest req, ServerResponse res) {
        Runtime rt = Runtime.getRuntime();
        String body = "# TYPE offboarding_jvm_memory_used_bytes gauge\n"
                + "offboarding_jvm_memory_used_bytes " + (rt.totalMemory() - rt.freeMemory()) + "\n"
                + "# TYPE offboarding_jvm_threads gauge\n"
                + "offboarding_jvm_threads " + ManagementFactory.getThreadMXBean().getThreadCount() + "\n"
                + "# TYPE offboarding_uptime_seconds gauge\n"
                + "offboarding_uptime_seconds " + (System.currentTimeMillis() - STARTED) / 1000 + "\n"
                + "# TYPE offboarding_sagas_compensated_total counter\n"
                + "offboarding_sagas_compensated_total " + COMPENSATED.get() + "\n";
        res.send(body);
    }
}
