package com.jrobertgardzinski.offboarding.infrastructure;

import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.lang.management.ManagementFactory;

/**
 * The service's vitals in Prometheus text format at {@code /metrics}, scraped by the workspace's
 * Prometheus (job "offboarding"). Hand-rolled to the house's lean taste, matching the manual
 * exporters in the formula backend and user-collections — the JVM's basics and uptime.
 */
final class MetricsEndpoint {

    private static final long STARTED = System.currentTimeMillis();

    private MetricsEndpoint() {
    }

    static void handle(ServerRequest req, ServerResponse res) {
        Runtime rt = Runtime.getRuntime();
        String body = "# TYPE offboarding_jvm_memory_used_bytes gauge\n"
                + "offboarding_jvm_memory_used_bytes " + (rt.totalMemory() - rt.freeMemory()) + "\n"
                + "# TYPE offboarding_jvm_threads gauge\n"
                + "offboarding_jvm_threads " + ManagementFactory.getThreadMXBean().getThreadCount() + "\n"
                + "# TYPE offboarding_uptime_seconds gauge\n"
                + "offboarding_uptime_seconds " + (System.currentTimeMillis() - STARTED) / 1000 + "\n";
        res.send(body);
    }
}
