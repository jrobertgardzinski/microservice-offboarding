package com.jrobertgardzinski.offboarding.infrastructure;

import au.com.dius.pact.consumer.MessagePactBuilder;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.core.model.messaging.Message;
import au.com.dius.pact.core.model.messaging.MessagePact;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static com.jrobertgardzinski.offboarding.infrastructure.RouterFixture.router;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What this orchestrator reads from microservice-memes' purge confirmation — {@code type} and
 * {@code email}; the participant's NAME comes from the topic, exactly as it did when security
 * orchestrated. Proven by driving the real router; verified against memes' real
 * confirmation-building code by its provider test.
 */
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-memes", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class MemesConfirmationContractTest {

    @Pact(consumer = "microservice-offboarding")
    MessagePact purgeConfirmation(MessagePactBuilder builder) {
        return builder.expectsToReceive("a user content purged confirmation")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "USER_CONTENT_PURGED")
                        .stringType("email", "leaver@example.com"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "purgeConfirmation")
    void theConfirmationAdvancesTheSaga(List<Message> messages) {
        RouterFixture fixture = router().withRunningSagaFor("leaver@example.com");
        fixture.router.handle("memes-events", messages.get(0).contentsAsString());
        assertTrue(fixture.store.all().get(0).confirmed.contains("memes"),
                "the memes confirmation must be recorded against the running saga");
    }
}
