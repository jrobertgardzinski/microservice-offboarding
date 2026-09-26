package com.jrobertgardzinski.offboarding.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * One overdue saga the sweeper re-commands instead of giving up on. {@code policy} is the
 * leaver's choices as stored at start — the verbatim JSON object, or null when the fact
 * carried none — so the re-command can repeat the original command instead of a bare default.
 *
 * <p>{@code retriesSoFar} is the counter AS THE SWEEP READ IT, and it travels with the candidate
 * so the charge that follows the delivery can name the round it is paying for. The charge used to
 * be a bare increment, which two sweepers offering the same candidate turned into two: the budget
 * a whole case is sized around halved, and nothing ever corrected it.
 */
public record Retry(UUID sagaId, String email, String policy, String initiatedBy,
                    int retriesSoFar, UUID userId) {
    public Retry(UUID sagaId, String email, String policy, String initiatedBy, int retriesSoFar) {
        this(sagaId, email, policy, initiatedBy, retriesSoFar, null);
    }

    public Retry(UUID sagaId, String email) {
        this(sagaId, email, null, null, 0);
    }

    public Retry(UUID sagaId, String email, String policy) {
        this(sagaId, email, policy, null, 0);
    }
}
