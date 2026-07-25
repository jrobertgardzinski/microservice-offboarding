package com.jrobertgardzinski.offboarding.infrastructure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The progress counters on /metrics: a delivered re-command and a failed sweeper pass must each
 * move their counter, because these two are how an operator tells a saga that is limping-but-
 * fighting (retries flowing) from a timeout path in real trouble (sweeps dying). The counters are
 * process-global, so the assertions read deltas, never absolutes.
 */
class MetricsEndpointTest {

    @Test
    void a_delivered_retry_moves_its_counter() {
        long before = counter("offboarding_retries_delivered_total");
        MetricsEndpoint.retryDelivered();
        assertEquals(before + 1, counter("offboarding_retries_delivered_total"));
    }

    @Test
    void a_failed_sweeper_pass_moves_its_counter() {
        long before = counter("offboarding_sweeper_pass_failures_total");
        MetricsEndpoint.sweeperPassFailed();
        assertEquals(before + 1, counter("offboarding_sweeper_pass_failures_total"));
    }

    @Test
    void both_counters_are_typed_for_prometheus() {
        String body = MetricsEndpoint.body();
        assertTrue(body.contains("# TYPE offboarding_retries_delivered_total counter"), body);
        assertTrue(body.contains("# TYPE offboarding_sweeper_pass_failures_total counter"), body);
    }

    private static long counter(String name) {
        for (String line : MetricsEndpoint.body().split("\n")) {
            if (line.startsWith(name + " ")) {
                return Long.parseLong(line.substring(name.length() + 1).trim());
            }
        }
        throw new AssertionError(name + " is not exposed on /metrics");
    }
}
