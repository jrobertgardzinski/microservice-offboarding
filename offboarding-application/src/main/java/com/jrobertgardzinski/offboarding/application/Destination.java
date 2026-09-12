package com.jrobertgardzinski.offboarding.application;

/**
 * Who an outgoing message is FOR — not where it goes.
 *
 * <p>The saga has exactly two audiences, and naming them is what lets the same orchestrator run in
 * two shapes. Between services these become topics; inside one process they become a call on an
 * in-memory bus. Neither of those is a decision this layer is entitled to make, and while it was
 * making it — {@code Outgoing} used to carry a topic name — the saga could not be run in a monolith
 * without inventing Kafka topics to talk to itself.
 */
public enum Destination {

    /** The content services that hold the leaver's things: mark, erase, or give it all back. */
    PARTICIPANTS,

    /**
     * Identity, which is waiting for exactly one sentence: the portal is clean, or it gave up.
     * Nothing else this service says is addressed here.
     */
    SECURITY
}
