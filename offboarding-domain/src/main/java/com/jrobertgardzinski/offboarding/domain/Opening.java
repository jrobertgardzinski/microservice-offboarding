package com.jrobertgardzinski.offboarding.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Everything a saga is opened WITH — the fields that have to be captured when the case opens
 * instead of being read back from configuration later.
 *
 * <p>{@code securitySagaId} is the handle security stamped on the deletion fact. It is stored
 * so the portal's verdict can ECHO it: without it security has to match verdicts by email
 * address, and a late verdict of a closed case then settles a NEWER deletion for the same
 * address. Null for a fact that carried no handle (an older producer).
 *
 * <p>{@code participants} is the required set AS CONFIGURED WHEN THIS SAGA OPENED. Null means
 * "not recorded" — a row from before the column existed, or a caller with no set to record —
 * and then completeness falls back to the caller's configuration, exactly as it used to.
 */
public record Opening(UUID factId, String email, String policy, UUID securitySagaId,
               Set<String> participants, String initiatedBy) {

    /** The pre-initiator spelling, for callers and tests with no basis to record. */
    public Opening(UUID factId, String email, String policy, UUID securitySagaId,
                   Set<String> participants) {
        this(factId, email, policy, securitySagaId, participants, null);
    }
}
