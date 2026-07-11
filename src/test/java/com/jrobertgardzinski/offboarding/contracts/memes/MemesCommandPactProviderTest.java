package com.jrobertgardzinski.offboarding.contracts.memes;

import au.com.dius.pact.provider.PactVerifyProvider;
import com.jrobertgardzinski.offboarding.infrastructure.CommandPactBase;
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

/**
 * This orchestrator inherited the {@code PURGE_USER_CONTENT} producer role from
 * microservice-security; memes' committed pact states which fields it reads, and this test proves
 * the REAL router still emits that shape. Skipped, not failed, when the consumer repo (or its
 * regenerated pact naming THIS provider) is not checked out next to this one.
 */
@Provider("microservice-offboarding")
@PactFolder("../microservice-memes/pacts")
@EnabledIf(value = "pactCheckedOut",
        disabledReason = "microservice-memes' offboarding pact is not checked out next to this repo")
public class MemesCommandPactProviderTest {

    static boolean pactCheckedOut() {
        return Files.exists(Path.of("../microservice-memes/pacts/microservice-memes-microservice-offboarding.json"));
    }

    @BeforeEach
    void target(PactVerificationContext context) {
        context.setTarget(new MessageTestTarget(List.of("com.jrobertgardzinski.offboarding.contracts.memes")));
    }

    @TestTemplate
    @ExtendWith(PactVerificationInvocationContextProvider.class)
    void theCommandShapeTheParticipantReliesOn(PactVerificationContext context) {
        context.verifyInteraction();
    }

    @PactVerifyProvider("a purge user content command")
    public String aPurgeUserContentCommand() {
        return CommandPactBase.realPurgeCommand();
    }

    @PactVerifyProvider("a purge user content command with an explicit policy")
    public String aPurgeUserContentCommandWithPolicy() {
        return CommandPactBase.realPurgeCommandWithPolicy();
    }
}
