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
 */
@Provider("microservice-offboarding")
@PactFolder("../microservice-security/pacts")
@EnabledIf(value = "pactCheckedOut",
        disabledReason = "microservice-security's offboarding pact is not checked out next to this repo")
class SecurityOutcomePactProviderTest {

    static boolean pactCheckedOut() {
        return Files.exists(Path.of("../microservice-security/pacts/microservice-security-microservice-offboarding.json"));
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
        fixture.router.handle(RouterFixture.FACTS_TOPIC,
                "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                        + "\"email\":\"leaver@example.com\",\"version\":1}");
        fixture.router.handle("memes-events", confirmation());
        fixture.router.handle("comments-events", confirmation());
        return fixture.router.handle("usercollections-events", confirmation())
                .get(0).payload();
    }

    @PactVerifyProvider("a portal purge failed announcement")
    public String aPortalPurgeFailedAnnouncement() {
        RouterFixture fixture = RouterFixture.router();
        // started a minute before the fixture's fixed "now", swept two-minutes-plus later — overdue
        fixture.store.start(UUID.randomUUID(), "leaver@example.com",
                java.time.Instant.parse("2026-07-11T11:00:00Z"));
        return fixture.router.sweepOverdue().get(0).payload();
    }

    private static String confirmation() {
        return "{\"type\":\"USER_CONTENT_PURGED\",\"email\":\"leaver@example.com\",\"version\":1}";
    }
}
