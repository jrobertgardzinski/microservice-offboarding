package com.jrobertgardzinski.offboarding.infrastructure;

import com.jrobertgardzinski.offboarding.application.Observation;
import com.jrobertgardzinski.offboarding.application.Observations;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The one class in this service that knows what a metric is called.
 *
 * <p>Everything above it states facts ({@link Observation}); here they are given this month's
 * spelling — {@code offboarding_sagas_compensated_total} and its two siblings — and
 * {@link MetricsEndpoint} reads them back out in Prometheus's text format. Swapping that format for
 * whatever comes next is rewriting these two files and nothing else.
 *
 * <p>All three are COUNTERS, which is a decision about the watching and not about the facts: they
 * answer "how many since this process started", never "how many right now", so none of them may
 * fall. A compensation that scrolled off a dashboard still happened.
 *
 * <p>An INSTANCE, where the three used to be static fields on the exporter that producers reached
 * into from two packages away. The composition root holds one and hands it to everything that
 * states a fact.
 */
public final class ExportedObservations implements Observations {

    private final AtomicLong compensated = new AtomicLong();
    private final AtomicLong retriesDelivered = new AtomicLong();
    private final AtomicLong sweeperPassFailures = new AtomicLong();

    @Override
    public void record(Observation observation) {
        switch (observation) {
            case Observation.SagaCompensated given -> compensated.addAndGet(given.count());
            case Observation.PurgeRetryDelivered ignored -> retriesDelivered.incrementAndGet();
            case Observation.SweepFailed ignored -> sweeperPassFailures.incrementAndGet();
        }
    }

    long compensated() {
        return compensated.get();
    }

    long retriesDelivered() {
        return retriesDelivered.get();
    }

    long sweeperPassFailures() {
        return sweeperPassFailures.get();
    }
}
