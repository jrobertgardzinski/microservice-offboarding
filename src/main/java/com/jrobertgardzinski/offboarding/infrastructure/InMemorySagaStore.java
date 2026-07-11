package com.jrobertgardzinski.offboarding.infrastructure;

import com.jrobertgardzinski.offboarding.application.SagaStore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** The test double: the same transition semantics as the JDBC store, in two maps. */
public class InMemorySagaStore implements SagaStore {

    /** One saga's mutable progress — package-visible for the tests' state fingerprints. */
    public static final class Saga {
        public final UUID id = UUID.randomUUID();
        public final UUID factId;
        public final String email;
        public String state = "STARTED";
        public final Instant createdAt;
        public final Set<String> confirmed = new HashSet<>();

        Saga(UUID factId, String email, Instant createdAt) {
            this.factId = factId;
            this.email = email;
            this.createdAt = createdAt;
        }
    }

    private final Map<UUID, Saga> sagas = new LinkedHashMap<>();

    @Override
    public UUID start(UUID factId, String email, Instant at) {
        return sagas.values().stream()
                .filter(saga -> saga.factId.equals(factId)).findFirst()
                .or(() -> running(email))
                .map(saga -> saga.id)
                .orElseGet(() -> {
                    Saga saga = new Saga(factId, email, at);
                    sagas.put(saga.id, saga);
                    return saga.id;
                });
    }

    @Override
    public boolean confirm(String email, String participant, Set<String> required, Instant at) {
        return running(email).map(saga -> {
            saga.confirmed.add(participant);
            if (saga.confirmed.containsAll(required)) {
                saga.state = "COMPLETED";   // the once-latch: running() no longer finds it
                return true;
            }
            return false;
        }).orElse(false);
    }

    @Override
    public boolean complete(String email, Instant at) {
        return running(email).map(saga -> {
            saga.state = "COMPLETED";
            return true;
        }).orElse(false);
    }

    @Override
    public List<String> compensateOverdue(Instant cutoff, Instant at) {
        List<String> emails = new ArrayList<>();
        for (Saga saga : sagas.values()) {
            if ("STARTED".equals(saga.state) && saga.createdAt.isBefore(cutoff)) {
                saga.state = "COMPENSATED";
                emails.add(saga.email);
            }
        }
        return emails;
    }

    /** The observable state, for the generic idempotence test's fingerprints. */
    public List<Saga> all() {
        return new ArrayList<>(sagas.values());
    }

    private java.util.Optional<Saga> running(String email) {
        return sagas.values().stream()
                .filter(saga -> saga.email.equals(email) && "STARTED".equals(saga.state))
                .findFirst();
    }
}
