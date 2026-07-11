package com.jrobertgardzinski.offboarding.contracts.comments;

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

/** The comments twin of the memes command provider test. */
@Provider("microservice-offboarding")
@PactFolder("../microservice-comments/pacts")
@EnabledIf(value = "pactCheckedOut",
        disabledReason = "microservice-comments' offboarding pact is not checked out next to this repo")
public class CommentsCommandPactProviderTest {

    static boolean pactCheckedOut() {
        return Files.exists(Path.of("../microservice-comments/pacts/microservice-comments-microservice-offboarding.json"));
    }

    @BeforeEach
    void target(PactVerificationContext context) {
        context.setTarget(new MessageTestTarget(List.of("com.jrobertgardzinski.offboarding.contracts.comments")));
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
