package com.jrobertgardzinski.offboarding.domain;

import java.util.List;

/** What one sweep pass decided about the overdue sagas. */
public record SweepResult(List<Retry> retries, List<Compensated> compensated) {
}
