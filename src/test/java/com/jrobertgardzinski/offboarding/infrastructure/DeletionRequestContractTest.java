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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The extracted saga's opening contract: security announces the FACT that an account requested
 * deletion, and this pact states exactly the fields the orchestrator reads — {@code id} (the
 * replay key), {@code email}, and the optional {@code policy} it ferries to the content services
 * untouched. Proven by driving the REAL router with the pact's payload; verified against
 * security's REAL fact-producing code by its provider tests. Tolerant reader: security may add
 * fields freely.
 */
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-security", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class DeletionRequestContractTest {

    @Pact(consumer = "microservice-offboarding")
    MessagePact deletionRequested(MessagePactBuilder builder) {
        return builder.expectsToReceive("an account deletion requested fact")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "ACCOUNT_DELETION_REQUESTED")
                        .uuid("id")
                        .stringType("email", "leaver@example.com"))
                .toPact();
    }

    @Pact(consumer = "microservice-offboarding")
    MessagePact deletionRequestedWithPolicy(MessagePactBuilder builder) {
        return builder.expectsToReceive("an account deletion requested fact with policy choices")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "ACCOUNT_DELETION_REQUESTED")
                        .uuid("id")
                        .stringType("email", "leaver@example.com")
                        .object("policy")
                        .stringType("memes", "DELETE")
                        .stringType("comments", "ANONYMIZE_AUTHOR")
                        .closeObject())
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "deletionRequested")
    void theFactOpensTheSagaAndCommandsThePurge(List<Message> messages) {
        RouterFixture fixture = router();
        List<EventsRouter.Outgoing> out =
                fixture.router.handle(RouterFixture.FACTS_TOPIC, messages.get(0).contentsAsString());
        assertEquals(1, out.size());
        assertEquals(EventsRouter.COMMANDS_TOPIC, out.get(0).topic());
        assertTrue(out.get(0).payload().contains("\"PURGE_USER_CONTENT\""));
    }

    @Test
    @PactTestFor(pactMethod = "deletionRequestedWithPolicy")
    void theLeaversChoicesRideTheCommand(List<Message> messages) {
        RouterFixture fixture = router();
        List<EventsRouter.Outgoing> out =
                fixture.router.handle(RouterFixture.FACTS_TOPIC, messages.get(0).contentsAsString());
        assertEquals(1, out.size());
        assertTrue(out.get(0).payload().contains("\"policy\""), "the choices must be ferried");
    }
}
