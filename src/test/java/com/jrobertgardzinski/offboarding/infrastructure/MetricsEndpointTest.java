package com.jrobertgardzinski.offboarding.infrastructure;

import com.jrobertgardzinski.offboarding.application.Observation;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The progress counters on /metrics: a delivered re-command and a failed sweep must each move their
 * counter, because these two are how an operator tells a saga that is limping-but-fighting (retries
 * flowing) from a timeout path in real trouble (sweeps dying).
 *
 * <p>The assertions read absolutes now, where they used to read deltas. That is the visible half of
 * the counters ceasing to be static: each test gets its own watcher, so "before + 1" — which was a
 * way of tolerating whatever the previous test had left behind — has nothing left to tolerate.
 */
@Epic("Infrastructure")
@Feature("Progress metrics")
class MetricsEndpointTest {

    private final ExportedObservations observations = new ExportedObservations();
    private final MetricsEndpoint metrics = new MetricsEndpoint(observations);

    @Test
    void a_delivered_retry_moves_its_counter() {
        observations.record(new Observation.PurgeRetryDelivered());

        assertEquals(1, counter("offboarding_retries_delivered_total"));
    }

    @Test
    void a_failed_sweeper_pass_moves_its_counter() {
        observations.record(new Observation.SweepFailed());

        assertEquals(1, counter("offboarding_sweeper_pass_failures_total"));
    }

    @Test
    void a_compensated_batch_counts_every_saga_in_it() {
        // reported per BATCH, because that is how the sweep decides — three abandoned deletions in
        // one pass are three outstanding legal requests, not one event
        observations.record(new Observation.SagaCompensated(3));

        assertEquals(3, counter("offboarding_sagas_compensated_total"));
    }

    @Test
    void both_counters_are_typed_for_prometheus() {
        String body = metrics.body();

        assertTrue(body.contains("# TYPE offboarding_retries_delivered_total counter"), body);
        assertTrue(body.contains("# TYPE offboarding_sweeper_pass_failures_total counter"), body);
    }

    private long counter(String name) {
        for (String line : metrics.body().split("\n")) {
            if (line.startsWith(name + " ")) {
                return Long.parseLong(line.substring(name.length() + 1).trim());
            }
        }
        throw new AssertionError(name + " is not exposed on /metrics");
    }
}
