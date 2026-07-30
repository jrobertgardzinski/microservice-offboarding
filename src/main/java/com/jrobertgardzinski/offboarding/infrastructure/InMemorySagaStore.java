package com.jrobertgardzinski.offboarding.infrastructure;

import com.jrobertgardzinski.offboarding.application.SagaStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** The test double: the same transition semantics as the JDBC store, in two maps. */
public class InMemorySagaStore implements SagaStore {

    /** One saga's mutable progress — package-visible for the tests' state fingerprints. */
    public static final class Saga {
        public final UUID id;
        public final UUID factId;
        public final String email;
        public String state = "STARTED";
        public final Instant createdAt;
        public Instant updatedAt;
        public final Set<String> confirmed = new HashSet<>();
        /** The mini-outbox flag: set only after the outcome demonstrably reached the broker. */
        public boolean announced;
        public int retries;
        /** The leaver's choices as stored at start — verbatim JSON, or null (see V3). */
        public final String policy;
        /** Security's handle on the deletion, echoed by the verdict — null when none (see V5). */
        public UUID securitySagaId;
        /** The quorum this saga opened with, or null when none was recorded (see V6). */
        public final Set<String> requiredParticipants;

        Saga(UUID factId, String email, String policy, UUID securitySagaId,
             Set<String> requiredParticipants, Instant createdAt) {
            this(UUID.randomUUID(), factId, email, policy, securitySagaId, requiredParticipants,
                    createdAt);
        }

        Saga(UUID id, UUID factId, String email, String policy, UUID securitySagaId,
             Set<String> requiredParticipants, Instant createdAt) {
            this.id = id;
            this.factId = factId;
            this.email = email;
            this.policy = policy;
            this.securitySagaId = securitySagaId;
            this.requiredParticipants = requiredParticipants == null
                    ? null : Set.copyOf(requiredParticipants);
            this.createdAt = createdAt;
            this.updatedAt = createdAt;
        }

        private boolean finished() {
            return "COMPLETED".equals(state) || "COMPENSATED".equals(state);
        }
    }

    private final Map<UUID, Saga> sagas = new LinkedHashMap<>();

    @Override
    public UUID start(Opening opening, Instant at) {
        Optional<Saga> replayed = sagas.values().stream()
                .filter(saga -> saga.factId.equals(opening.factId())).findFirst();
        if (replayed.isPresent()) {
            return replayed.get().id;
        }
        Optional<Saga> running = running(opening.email());
        if (running.isPresent()) {
            // a joining fact hands the running saga the handle of the security saga now waiting —
            // mirrors the JDBC adapter, including leaving it alone when the fact carried none
            if (opening.securitySagaId() != null) {
                running.get().securitySagaId = opening.securitySagaId();
            }
            return running.get().id;
        }
        Saga saga = new Saga(opening.factId(), opening.email(), opening.policy(),
                opening.securitySagaId(), opening.participants(), at);
        sagas.put(saga.id, saga);
        return saga.id;
    }

    @Override
    public Optional<Recorded> confirm(String email, UUID sagaId, String participant,
                                      Set<String> required, Instant at) {
        // the saga id, when echoed, is the precise address AND the final word: a stale id (the
        // saga no longer STARTED) is a stray from a closed case, never an email fallback — the
        // fallback exists solely for confirmations without the field. Mirrors the JDBC adapter.
        Optional<Saga> target = sagaId != null
                ? Optional.ofNullable(sagas.get(sagaId)).filter(saga -> "STARTED".equals(saga.state))
                : running(email);
        return target.map(saga -> {
            saga.confirmed.add(participant);
            // the quorum the saga opened with wins over the caller's configuration whenever it
            // recorded one — mirrors the JDBC adapter
            Set<String> quorum = saga.requiredParticipants == null
                    ? required : saga.requiredParticipants;
            if (saga.confirmed.containsAll(quorum)) {
                saga.state = "COMPLETED";   // the once-latch: running() no longer finds it
                saga.updatedAt = at;
                return new Recorded(saga.id, saga.securitySagaId, true);
            }
            return new Recorded(saga.id, saga.securitySagaId, false);
        });
    }

    @Override
    public boolean complete(String email, Instant at) {
        return running(email).map(saga -> {
            saga.state = "COMPLETED";
            saga.updatedAt = at;
            return true;
        }).orElse(false);
    }

    @Override
    public SweepResult sweepOverdue(Instant cutoff, int maxRetries, Instant at) {
        List<Retry> retries = new ArrayList<>();
        List<Compensated> compensated = new ArrayList<>();
        for (Saga saga : sagas.values()) {
            // updatedAt, not createdAt: the deadline runs from the last delivered re-command —
            // mirrors the JDBC adapter (see SagaStore#sweepOverdue for why birth was wrong)
            if ("STARTED".equals(saga.state) && saga.updatedAt.isBefore(cutoff)) {
                if (saga.retries < maxRetries) {
                    // a candidate, not a charge: retryDelivered() moves the counter once the
                    // re-command reached the broker — mirrors the JDBC adapter. The stored
                    // policy rides along so the re-command repeats the original
                    retries.add(new Retry(saga.id, saga.email, saga.policy));
                } else {
                    saga.state = "COMPENSATED";
                    saga.updatedAt = at;
                    compensated.add(new Compensated(saga.id, saga.email, Set.copyOf(saga.confirmed),
                            saga.securitySagaId));
                }
            }
        }
        return new SweepResult(retries, compensated);
    }

    @Override
    public boolean retryDelivered(UUID sagaId, Instant at) {
        Saga saga = sagas.get(sagaId);
        if (saga != null && "STARTED".equals(saga.state)) {
            saga.retries++;
            // the stamp that gives the participant its budget: the sweep's overdue clock restarts
            // here — mirrors the JDBC adapter, the caller's instant in both
            saga.updatedAt = at;
            return true;   // charged — mirrors the JDBC adapter's updated-row count
        }
        return false;      // the no-op on a finished or unknown saga: nothing to meter
    }

    @Override
    public void markAnnounced(UUID sagaId) {
        Saga saga = sagas.get(sagaId);
        if (saga != null) {
            saga.announced = true;
        }
    }

    @Override
    public List<PendingOutcome> unannouncedOutcomes(Instant olderThan) {
        return sagas.values().stream()
                .filter(saga -> saga.finished() && !saga.announced && saga.updatedAt.isBefore(olderThan))
                .map(saga -> new PendingOutcome(saga.id, saga.email, saga.state,
                        "COMPENSATED".equals(saga.state) ? Set.copyOf(saga.confirmed) : Set.<String>of(),
                        saga.securitySagaId))
                .toList();
    }

    @Override
    public int deleteFinishedBefore(Instant olderThan) {
        List<UUID> gone = sagas.values().stream()
                .filter(saga -> saga.finished() && saga.announced && saga.updatedAt.isBefore(olderThan))
                .map(saga -> saga.id)
                .toList();
        gone.forEach(sagas::remove);
        return gone.size();
    }

    /**
     * Test seeding only: open a running saga under a KNOWN id — what a contract test needs so a
     * recorded confirmation example can echo the id of THE saga (a stale or unknown echoed id is
     * a stray by design; see the JDBC adapter's confirm()).
     */
    UUID startWithId(UUID sagaId, UUID factId, String email, Instant at) {
        return startWithId(sagaId, factId, email, null, at);
    }

    /** The same seeding, with security's handle on the deletion — for the verdict-echo tests. */
    UUID startWithId(UUID sagaId, UUID factId, String email, UUID securitySagaId, Instant at) {
        Saga saga = new Saga(sagaId, factId, email, null, securitySagaId, null, at);
        sagas.put(saga.id, saga);
        return saga.id;
    }

    /** The observable state, for the generic idempotence test's fingerprints. */
    public List<Saga> all() {
        return new ArrayList<>(sagas.values());
    }

    private Optional<Saga> running(String email) {
        return sagas.values().stream()
                .filter(saga -> saga.email.equals(email) && "STARTED".equals(saga.state))
                .findFirst();
    }
}
