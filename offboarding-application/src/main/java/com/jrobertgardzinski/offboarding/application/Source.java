package com.jrobertgardzinski.offboarding.application;

/**
 * Who an incoming message is FROM — again, not where it arrived.
 *
 * <p>A message reaches this saga as one of two things: the fact that identity opened a deletion, or
 * one participant's word that it has done as it was told. Which one it is used to be decided by
 * comparing topic names, which meant the orchestrator held the deployment's topology and a monolith
 * had to fake it.
 *
 * <p>Mapping an arrival to a source is the adapter's job, and it is deployment topology by nature:
 * between services it reads a topic ({@code OFFBOARDING_PARTICIPANTS} says which topic is whose),
 * in one process it reads whichever module handed the message over.
 */
public sealed interface Source {

    /** Identity announcing that somebody's account is going. */
    record Security() implements Source {
    }

    /**
     * One content participant, by the name the saga counts confirmations under. The name matters
     * and the transport does not: the quorum is a set of these.
     */
    record Participant(String name) implements Source {
    }

    Source SECURITY = new Security();

    static Source participant(String name) {
        return new Participant(name);
    }
}
