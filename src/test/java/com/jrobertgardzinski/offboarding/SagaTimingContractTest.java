package com.jrobertgardzinski.offboarding;

import com.jrobertgardzinski.offboarding.application.SweepOverdue;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one arithmetic nobody may change alone.
 *
 * <p>This service decides a case within {@code purge timeout x (retries + 1)}. Security runs its
 * own safety net for a dead orchestrator, and that net must fire AFTER this — otherwise the
 * account is unlocked and apologised for while the portal is still working, and the purge then
 * lands on a restored account. Measured on the live stack on 2026-08-08: the account came back at
 * ~5 minutes, its content went at ~8. Nobody decided that; it was the sum of two timeouts in two
 * repositories that nobody read together, each with a comment explaining only its own half.
 *
 * <p>So the rule is a test now, and it is on THIS side on purpose: this is the side that can make
 * the case longer. Security's number is repeated here as a constant because a contract has to name
 * both parties — if that default moves, this test is what says so out loud.
 */
@Epic("Saga")
@Feature("Timing contract with security")
class SagaTimingContractTest {

    /**
     * {@code account-deletion.purge-timeout} in microservice-security (AccountDeletionOrchestrator).
     * Not read from there — the services are deliberately independent, and importing identity's
     * configuration to check it would be the coupling this contract exists to avoid.
     */
    private static final Duration SECURITY_GIVES_UP_AFTER = Duration.ofMinutes(12);

    @Test
    @DisplayName("the whole case is decided before security's safety net fires")
    void the_portal_finishes_first() {
        assertTrue(SweepOverdue.worstCaseDecision().compareTo(SECURITY_GIVES_UP_AFTER) < 0,
                "this service takes up to " + SweepOverdue.worstCaseDecision().toMinutes()
                        + " minutes to decide, and security gives up after "
                        + SECURITY_GIVES_UP_AFTER.toMinutes() + ". Raising the timeout or the"
                        + " retry count past that hands the account back before its content is"
                        + " gone — raise account-deletion.purge-timeout in microservice-security"
                        + " FIRST, then this constant, then the number you came here to change");
    }

    @Test
    @DisplayName("the worst case is the product it claims to be, not a number typed twice")
    void the_worst_case_is_derived() {
        assertEquals(SweepOverdue.DEFAULT_PURGE_TIMEOUT.multipliedBy(
                        SweepOverdue.DEFAULT_MAX_RETRIES + 1L),
                SweepOverdue.worstCaseDecision());
        assertEquals(Duration.ofMinutes(8), SweepOverdue.worstCaseDecision(),
                "8 minutes is the figure both services' javadocs quote; if this fails, one of"
                        + " those explanations has quietly become fiction");
    }
}
