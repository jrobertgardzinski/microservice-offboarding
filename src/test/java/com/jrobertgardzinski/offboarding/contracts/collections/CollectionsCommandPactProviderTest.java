package com.jrobertgardzinski.offboarding.contracts.collections;

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

/** The user-collections twin of the memes command provider test. */
@Provider("microservice-offboarding")
@PactFolder("../microservice-user-collections/pacts")
@EnabledIf(value = "pactCheckedOut",
        disabledReason = "microservice-user-collections' offboarding pact is not checked out next to this repo")
public class CollectionsCommandPactProviderTest {

    static boolean pactCheckedOut() {
        return Files.exists(Path.of(
                "../microservice-user-collections/pacts/microservice-user-collections-microservice-offboarding.json"));
    }

    @BeforeEach
    void target(PactVerificationContext context) {
        context.setTarget(new MessageTestTarget(List.of("com.jrobertgardzinski.offboarding.contracts.collections")));
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
}
