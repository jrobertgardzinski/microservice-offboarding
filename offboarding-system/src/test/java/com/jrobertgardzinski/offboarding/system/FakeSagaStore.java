package com.jrobertgardzinski.offboarding.system;

import com.jrobertgardzinski.closure.ClosureInitiator;
import com.jrobertgardzinski.offboarding.domain.Compensated;
import com.jrobertgardzinski.offboarding.domain.Opening;
import com.jrobertgardzinski.offboarding.domain.PendingOutcome;
import com.jrobertgardzinski.offboarding.domain.Recorded;
import com.jrobertgardzinski.offboarding.domain.Retry;
import com.jrobertgardzinski.offboarding.domain.SweepResult;
import com.jrobertgardzinski.offboarding.system.SagaStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * A test double, and it lives with the tests on purpose. It used to sit in main beside the JDBC
 * adapter, where nothing in production ever constructed it — the identity service already keeps
 * its in-memory doubles in the module that drives the specs, and this is the same rule.
 *
 * <p>Published in this module's test-jar so the boundary tests, which drive the real loop against
 * a real broker, can still use one store instead of growing a second copy.
 * The test double: the same transition semantics as the JDBC store, in two maps. */
public class FakeSagaStore implements SagaStore {

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
        /** The leaver's choices as stored at start — verbatim JSON, or null (see V3). Not final:
         *  an ADMIN's case that the account's own owner then asks for loses its conditions. */
        public String policy;
        /** Security's handle on the deletion, echoed by the verdict — null when none (see V5). */
        public UUID securitySagaId;
        /** The quorum this saga opened with, or null when none was recorded (see V6). */
        public final Set<String> requiredParticipants;
        /** Who asked for the closure — decides whether the policy may be honoured (see V7). */
        public String initiatedBy;
        /** The leaver's identity as security stated it; null on a saga opened by an older fact. */
        public final UUID userId;

        Saga(UUID factId, String email, String policy, UUID securitySagaId,
             Set<String> requiredParticipants, Instant createdAt) {
            this(UUID.randomUUID(), factId, email, policy, securitySagaId, requiredParticipants,
                    createdAt, null);
        }

        Saga(UUID id, UUID factId, String email, String policy, UUID securitySagaId,
             Set<String> requiredParticipants, Instant createdAt) {
            this(id, factId, email, policy, securitySagaId, requiredParticipants, createdAt, null);
        }

        Saga(UUID id, UUID factId, String email, String policy, UUID securitySagaId,
             Set<String> requiredParticipants, Instant createdAt, String initiatedBy) {
            this(id, factId, email, policy, securitySagaId, requiredParticipants, createdAt, initiatedBy, null);
        }

        Saga(UUID id, UUID factId, String email, String policy, UUID securitySagaId,
             Set<String> requiredParticipants, Instant createdAt, String initiatedBy, UUID userId) {
            this.userId = userId;
            this.initiatedBy = initiatedBy;
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
    /** Every fact ever answered with a saga id, the one that OPENED it and the ones that merely
     *  joined it — mirrors V8, without which a joining fact's redelivery forks a second saga
     *  once the case it joined has finished. */
    private final Map<UUID, UUID> sagaByFact = new LinkedHashMap<>();

    @Override
    public UUID start(Opening opening, Instant at) {
        UUID replayed = sagaByFact.get(opening.factId());
        if (replayed != null) {
            return replayed;
        }
        Optional<Saga> running = running(opening.email());
        if (running.isPresent()) {
            // a joining fact hands the running saga the handle of the security saga now waiting —
            // mirrors the JDBC adapter, including leaving it alone when the fact carried none
            if (opening.securitySagaId() != null) {
                running.get().securitySagaId = opening.securitySagaId();
            }
            // and the one other thing a joining fact may change: an administrator's closure that
            // the OWNER then asks for themselves becomes the owner's, conditions dropped. Only
            // ever ADMIN → SELF; mirrors the JDBC adapter (see JdbcSagaStore#adoptSelfRequest)
            if (ClosureInitiator.of(opening.initiatedBy()) == ClosureInitiator.SELF
                    && ClosureInitiator.of(running.get().initiatedBy) == ClosureInitiator.ADMIN) {
                running.get().initiatedBy = ClosureInitiator.SELF.wire();
                running.get().policy = null;
            }
            sagaByFact.put(opening.factId(), running.get().id);   // the join, remembered (V8)
            return running.get().id;
        }
        Saga saga = new Saga(UUID.randomUUID(), opening.factId(), opening.email(), opening.policy(),
                opening.securitySagaId(), opening.participants(), at, opening.initiatedBy(),
                opening.userId());
        sagas.put(saga.id, saga);
        sagaByFact.put(saga.factId, saga.id);
        return saga.id;
    }

    @Override
    public Optional<Recorded> confirm(UUID userId, UUID sagaId, String participant,
                                      Set<String> required, Instant at) {
        // the saga id, when echoed, is the precise address AND the final word: a stale id (the
        // saga no longer STARTED) is a stray from a closed case, never an email fallback — the
        // fallback exists solely for confirmations without the field. Mirrors the JDBC adapter.
        Optional<Saga> target = sagaId != null
                ? Optional.ofNullable(sagas.get(sagaId)).filter(saga -> "STARTED".equals(saga.state))
                : runningOf(userId);
        return target.map(saga -> {
            saga.confirmed.add(participant);
            // the quorum the saga opened with wins over the caller's configuration whenever it
            // recorded one — mirrors the JDBC adapter
            Set<String> quorum = saga.requiredParticipants == null
                    ? required : saga.requiredParticipants;
            if (saga.confirmed.containsAll(quorum)) {
                saga.state = "COMPLETED";   // the once-latch: running() no longer finds it
                saga.updatedAt = at;
                // the policy rides back out with the completing confirmation: the caller sends
                // the CLOSURE command next, and that is what carries it to the participants
                return new Recorded(saga.id, saga.securitySagaId, true, saga.policy, saga.initiatedBy,
                        saga.userId, saga.email);
            }
            return new Recorded(saga.id, saga.securitySagaId, false, saga.policy, saga.initiatedBy,
                    saga.userId, saga.email);
        });
    }

    @Override
    public boolean complete(UUID sagaId, Instant at) {
        // the saga named, and only while it waits for nobody — mirrors the JDBC adapter, including
        // the null quorum (a saga that recorded none) deferring to the caller
        Saga saga = sagas.get(sagaId);
        if (saga == null || !"STARTED".equals(saga.state)
                || (saga.requiredParticipants != null && !saga.requiredParticipants.isEmpty())) {
            return false;
        }
        saga.state = "COMPLETED";
        saga.updatedAt = at;
        return true;
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
                    retries.add(new Retry(saga.id, saga.email, saga.policy, saga.initiatedBy,
                            saga.retries, saga.userId));
                } else {
                    saga.state = "COMPENSATED";
                    saga.updatedAt = at;
                    compensated.add(new Compensated(saga.id, saga.email, Set.copyOf(saga.confirmed),
                            saga.securitySagaId, saga.userId));
                }
            }
        }
        return new SweepResult(retries, compensated);
    }

    @Override
    public boolean retryDelivered(UUID sagaId, int retriesSoFar, Instant at) {
        Saga saga = sagas.get(sagaId);
        // the round the sweep offered, not just any round — mirrors the JDBC adapter's
        // "AND retries = ?", without which two sweepers charge one re-command twice
        if (saga != null && "STARTED".equals(saga.state) && saga.retries == retriesSoFar) {
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
                        saga.securitySagaId, saga.policy, saga.initiatedBy, saga.userId))
                .toList();
    }

    @Override
    public int deleteFinishedBefore(Instant olderThan) {
        List<UUID> gone = sagas.values().stream()
                .filter(saga -> saga.finished() && saga.announced && saga.updatedAt.isBefore(olderThan))
                .map(saga -> saga.id)
                .toList();
        gone.forEach(sagas::remove);
        sagaByFact.values().removeIf(gone::contains);
        return gone.size();
    }

    /**
     * Test seeding only: open a running saga under a KNOWN id — what a contract test needs so a
     * recorded confirmation example can echo the id of THE saga (a stale or unknown echoed id is
     * a stray by design; see the JDBC adapter's confirm()).
     */
    public UUID startWithId(UUID sagaId, UUID factId, String email, UUID userId, Instant at) {
        return startWithId(sagaId, factId, email, userId, null, at);
    }

    /** The same seeding, with security's handle on the deletion — for the verdict-echo tests. */
    public UUID startWithId(UUID sagaId, UUID factId, String email, UUID userId, UUID securitySagaId, Instant at) {
        Saga saga = new Saga(sagaId, factId, email, null, securitySagaId, null, at, null, userId);
        sagas.put(saga.id, saga);
        sagaByFact.put(saga.factId, saga.id);
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

    private Optional<Saga> runningOf(UUID userId) {
        return sagas.values().stream()
                .filter(saga -> userId != null && userId.equals(saga.userId) && "STARTED".equals(saga.state))
                .findFirst();
    }
}
