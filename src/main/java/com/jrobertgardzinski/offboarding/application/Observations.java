package com.jrobertgardzinski.offboarding.application;

/**
 * Where this service's {@link Observation}s go — the one port between "the code has noticed
 * something" and whatever is watching this month.
 *
 * <p>The narrowest interface that can carry every fact: a caller states WHAT happened and nothing
 * about how it should be counted, named or alerted on. Those are the watching tool's decisions, and
 * they are exactly what changes when the tool does.
 */
public interface Observations {

    /**
     * What this service does when nothing is watching: it goes on working. The boundary made real
     * rather than described — a service with no watcher is not a broken one, so the port has an
     * answer even with no adapter assembled.
     *
     * <p>It states nothing anywhere on purpose, not even a log line: a "nobody is listening"
     * warning once a minute is itself a watcher, and a noisy one.
     */
    Observations SILENT = observation -> { };

    void record(Observation observation);
}
