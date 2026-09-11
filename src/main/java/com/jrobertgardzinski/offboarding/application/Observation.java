package com.jrobertgardzinski.offboarding.application;

/**
 * Something this orchestrator has noticed about itself and considers worth saying out loud — a
 * domain probe: the code states a fact in the language of the business, and whatever is watching
 * translates it into its own.
 *
 * <p>Sealed because it is a vocabulary, not an extension point. Every fact here is a sentence about
 * account deletion that <strong>no tool could derive on its own</strong>: a tracing agent knows a
 * method took 40ms, but nothing outside this service can know that a deletion was given up on,
 * because that is a conclusion drawn from this service's own rules.
 *
 * <p>It lives beside {@link SagaStore} rather than in a domain package of its own, because that is
 * where this service keeps its contracts: it is a process manager, and the saga IS its model.
 *
 * <p>Memory, threads and uptime are absent on purpose. They are properties of a process, readable
 * by anything that can see it, and spelled in whatever vocabulary this year's tool uses. What is in
 * here is what survives replacing that tool.
 */
public sealed interface Observation {

    /**
     * The sweeper gave up on this many sagas at once — the rarest and worst fact this service
     * states. It means an account deletion did NOT happen: the account is unlocked, the content is
     * restored and the person has been told "it did not work", so a legal request is outstanding
     * and somebody has to find out why. Reported per batch, because that is how the sweep decides.
     */
    record SagaCompensated(int count) implements Observation {
    }

    /**
     * One re-commanded purge demonstrably reached the broker. The saga is limping and still
     * fighting — worth seeing next to the one above, because a rising count here with no
     * compensations is a participant that is slow, not lost.
     */
    record PurgeRetryDelivered() implements Observation {
    }

    /**
     * One sweep died on infrastructure and will be retried after the backoff. Not a saga's failure
     * but the failure of the thing that would have NOTICED one, which is why it is said at all: a
     * sweeper that cannot run is a timeout nobody is counting down.
     */
    record SweepFailed() implements Observation {
    }
}
