package com.jrobertgardzinski.offboarding.entity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Where one confirmation landed. {@code completedSaga} is the once-latch: true only for the
 * call that turned the LAST required confirmation into COMPLETED. A confirmation that landed
 * on no saga at all is {@link Optional#empty()} — a stray, recorded NOWHERE, which is a
 * different event from "recorded, not complete yet" and deserves its own log line.
 * {@code securitySagaId} is the handle the verdict must echo (null when the saga has none), and
 * {@code policy} is the leaver's stored choices, which the CLOSURE command carries back to the
 * participants — they apply their rule at erasure time, so the last confirmation is exactly
 * when that policy is needed again.
 */
public record Recorded(UUID sagaId, UUID securitySagaId, boolean completedSaga, String policy,
                String initiatedBy) {

    /** The pre-closure spelling, for callers and tests that have no policy to carry. */
    public Recorded(UUID sagaId, UUID securitySagaId, boolean completedSaga) {
        this(sagaId, securitySagaId, completedSaga, null, null);
    }

    /** The pre-initiator spelling. */
    public Recorded(UUID sagaId, UUID securitySagaId, boolean completedSaga, String policy) {
        this(sagaId, securitySagaId, completedSaga, policy, null);
    }
}
