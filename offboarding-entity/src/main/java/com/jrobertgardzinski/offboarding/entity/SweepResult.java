package com.jrobertgardzinski.offboarding.entity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/** What one sweep pass decided about the overdue sagas. */
public record SweepResult(List<Retry> retries, List<Compensated> compensated) {
}
