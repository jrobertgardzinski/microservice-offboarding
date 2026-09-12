package com.jrobertgardzinski.offboarding.boundary;

import com.jrobertgardzinski.offboarding.control.Destination;
import com.jrobertgardzinski.offboarding.control.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Where the saga's two audiences and two kinds of caller actually live on this deployment.
 *
 * <p>This is the whole of what used to sit in the orchestrator: a facts topic, a topic per
 * participant, and the two topics it publishes to. None of it is a decision about account deletion
 * — it is where the wires run — and while the router held it, the saga could not be assembled any
 * other way. A monolith would have had to invent topics to let a process talk to itself.
 *
 * <p>So the mapping lives here, in the only module that knows what a topic is, and the router now
 * speaks of {@link Source} and {@link Destination}. Swapping this class for an in-memory bus is
 * what "the same saga in a monolith" means in practice.
 */
final class SagaTopics {

    /** What the participants are told to do; every command the saga sends rides this one. */
    static final String CONTENT_COMMANDS = "content-commands";

    /** The single verdict identity waits for — purged, or given up on. */
    static final String OFFBOARDING_EVENTS = "offboarding-events";

    private final String factsTopic;
    private final Map<String, String> participantByTopic;

    SagaTopics(String factsTopic, Map<String, String> participantByTopic) {
        this.factsTopic = factsTopic;
        this.participantByTopic = Map.copyOf(participantByTopic);
    }

    /** Everything the loop must subscribe to: the participants' topics, and security's. */
    List<String> subscriptions() {
        List<String> topics = new ArrayList<>(participantByTopic.keySet());
        topics.add(factsTopic);
        return topics;
    }

    /**
     * Which caller a record came from. An unknown topic answers null rather than throwing: the loop
     * is subscribed to exactly what this class listed, so a record from anywhere else is a
     * misconfiguration to be dropped, not an exception to wedge the partition with.
     */
    Source senderOf(String topic) {
        if (factsTopic.equals(topic)) {
            return Source.SECURITY;
        }
        String participant = participantByTopic.get(topic);
        return participant == null ? null : Source.participant(participant);
    }

    /** Where an audience is reached on this deployment. */
    String topicFor(Destination destination) {
        return switch (destination) {
            case PARTICIPANTS -> CONTENT_COMMANDS;
            case SECURITY -> OFFBOARDING_EVENTS;
        };
    }
}
