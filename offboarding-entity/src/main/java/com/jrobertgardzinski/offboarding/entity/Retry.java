package com.jrobertgardzinski.offboarding.entity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * One overdue saga the sweeper re-commands instead of giving up on. {@code policy} is the
 * leaver's choices as stored at start — the verbatim JSON object, or null when the fact
 * carried none — so the re-command can repeat the original command instead of a bare default.
 */
public record Retry(UUID sagaId, String email, String policy, String initiatedBy) {
    public Retry(UUID sagaId, String email) {
        this(sagaId, email, null, null);
    }

    public Retry(UUID sagaId, String email, String policy) {
        this(sagaId, email, policy, null);
    }
}
