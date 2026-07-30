package com.jrobertgardzinski.offboarding.infrastructure;

import au.com.dius.pact.provider.PactVerifyProvider;
import au.com.dius.pact.provider.junit5.MessageTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.loader.PactFolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * The single outcome security waits for, provider side: security's committed pact states which
 * fields it reads from {@code PORTAL_CONTENT_PURGED} / {@code PORTAL_PURGE_FAILED}, and this test
 * proves the REAL router emits those shapes — the completion by walking a saga through its last
 * confirmation, the failure by sweeping an overdue one. Skipped when security's offboarding pact
 * is not checked out next to this repo.
 *
 * <p>Note where that pact lives: the three CONTENT participants are siblings inside the portal
 * workspace ({@code ../microservice-memes} and friends), but security belongs to the shared
 * identity stack one level up — the same {@code ../shared} the compose file includes and
 * {@code infra-up.sh} builds first. This test used to look for it among the portal siblings, where
 * it has not been since the workspace split, so it silently skipped on every run: a contract test
 * that never runs is indistinguishable from one that passes, which is the worst thing a contract
 * test can be.
 */
@Provider("microservice-offboarding")
@PactFolder(SecurityOutcomePactProviderTest.PACT_FOLDER)
@EnabledIf(value = "pactCheckedOut",
        disabledReason = "microservice-security's offboarding pact is not checked out in ../../shared")
class SecurityOutcomePactProviderTest {

    /** Relative to the MODULE directory, which is what surefire makes the working directory. */
    static final String PACT_FOLDER = "../../shared/microservice-security/pacts";

    static boolean pactCheckedOut() {
        return Files.exists(
                Path.of(PACT_FOLDER, "microservice-security-microservice-offboarding.json"));
    }

    @BeforeEach
    void target(PactVerificationContext context) {
        context.setTarget(new MessageTestTarget(List.of("com.jrobertgardzinski")));
    }

    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void theOutcomeShapeSecurityReliesOn(PactVerificationContext context) {
        context.verifyInteraction();
    }

    @PactVerifyProvider("a portal content purged announcement")
    public String aPortalContentPurgedAnnouncement() {
        RouterFixture fixture = RouterFixture.router();
        // the fact carries security's OWN saga id, exactly as its orchestrator writes it — the
        // verdict below echoes it back, so the example is the real payload, correlation and all
        fixture.router.handle(RouterFixture.FACTS_TOPIC,
                "{\"id\":\"" + UUID.randomUUID() + "\",\"sagaId\":\"" + UUID.randomUUID() + "\","
                        + "\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");
        fixture.router.handle("memes-events", confirmation());
        fixture.router.handle("comments-events", confirmation());
        return fixture.router.handle("usercollections-events", confirmation())
                .get(0).payload();
    }

    @PactVerifyProvider("a portal purge failed announcement")
    public String aPortalPurgeFailedAnnouncement() {
        // no retry budget: the sweeper's road to capitulation is a story about a MOVING clock (each
        // delivered re-command buys the participant another whole timeout), and this fixture's
        // clock is fixed. The example needs the verdict, so let one sweep produce it
        RouterFixture fixture = RouterFixture.routerCapitulatingAtOnce();
        // started an hour before the fixture's fixed "now" and untouched since — overdue
        fixture.store.start(new com.jrobertgardzinski.offboarding.application.SagaStore.Opening(
                        UUID.randomUUID(), "leaver@example.com", null, UUID.randomUUID(),
                        java.util.Set.copyOf(RouterFixture.PARTICIPANT_BY_TOPIC.values())),
                java.time.Instant.parse("2026-07-11T11:00:00Z"));
        return fixture.router.sweepOverdue().stream()
                .filter(outgoing -> EventsRouter.OUTCOMES_TOPIC.equals(outgoing.topic()))
                .findFirst().orElseThrow().payload();
    }

    private static String confirmation() {
        return "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"leaver@example.com\",\"version\":1}";
    }
}
