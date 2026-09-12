package com.jrobertgardzinski.offboarding.boundary;

import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;

import java.lang.management.ManagementFactory;

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

    private final ExportedObservations observations;

    MetricsEndpoint(ExportedObservations observations) {
        this.observations = observations;
    }

    void handle(ServerRequest req, ServerResponse res) {
        res.send(body());
    }

    /**
     * The exposition text — separate from the HTTP plumbing so the test can read the counters.
     *
     * <p>The three business lines come from {@link ExportedObservations}, which is the only thing
     * in this service that decides a compensated saga is spelled as a counter. The JVM lines above
     * them stay where they are: memory, threads and uptime are properties of a process that
     * anything can read, not facts this service knows.
     */
    String body() {
        Runtime rt = Runtime.getRuntime();
        return "# TYPE offboarding_jvm_memory_used_bytes gauge\n"
                + "offboarding_jvm_memory_used_bytes " + (rt.totalMemory() - rt.freeMemory()) + "\n"
                + "# TYPE offboarding_jvm_threads gauge\n"
                + "offboarding_jvm_threads " + ManagementFactory.getThreadMXBean().getThreadCount() + "\n"
                + "# TYPE offboarding_uptime_seconds gauge\n"
                + "offboarding_uptime_seconds " + (System.currentTimeMillis() - STARTED) / 1000 + "\n"
                + "# TYPE offboarding_sagas_compensated_total counter\n"
                + "offboarding_sagas_compensated_total " + observations.compensated() + "\n"
                + "# TYPE offboarding_retries_delivered_total counter\n"
                + "offboarding_retries_delivered_total " + observations.retriesDelivered() + "\n"
                + "# TYPE offboarding_sweeper_pass_failures_total counter\n"
                + "offboarding_sweeper_pass_failures_total " + observations.sweeperPassFailures() + "\n";
    }
}
