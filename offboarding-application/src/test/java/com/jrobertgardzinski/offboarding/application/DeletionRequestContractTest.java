package com.jrobertgardzinski.offboarding.application;

import com.jrobertgardzinski.offboarding.application.Destination;
import com.jrobertgardzinski.offboarding.application.Source;
import au.com.dius.pact.consumer.MessagePactBuilder;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.core.model.messaging.Message;
import au.com.dius.pact.core.model.messaging.MessagePact;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static com.jrobertgardzinski.offboarding.application.RouterFixture.router;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The extracted saga's opening contract: security announces the FACT that an account requested
 * deletion, and this pact states exactly the fields the orchestrator reads — {@code id} (the
 * replay key), {@code sagaId} (security's own handle on the deletion, stored with the saga and
 * echoed by the verdict), {@code email}, and the optional {@code policy} it ferries to the content
 * services untouched. Proven by driving the REAL router with the pact's payload; verified against
 * security's REAL fact-producing code by its provider tests. Tolerant reader: security may add
 * fields freely.
 */
@Epic("Contract")
@Feature("Deletion request")
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
                        // security's own saga id: stored with the portal's saga and echoed by the
                        // verdict, so security can settle THE request this verdict is about
                        // instead of matching it to the address
                        .uuid("sagaId")
                        .stringType("email", "leaver@example.com")
                        // WHO asked, which is what this consumer ferries onto every command it
                        // sends: the content services honour a rule only for an administrator's
                        // closure, and read anything else as the account owner's own request
                        .stringValue("initiatedBy", "SELF"))
                .toPact();
    }

    @Pact(consumer = "microservice-offboarding")
    MessagePact deletionRequestedWithPolicy(MessagePactBuilder builder) {
        return builder.expectsToReceive("an account deletion requested fact with policy choices")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "ACCOUNT_DELETION_REQUESTED")
                        .uuid("id")
                        // security's own saga id: stored with the portal's saga and echoed by the
                        // verdict, so security can settle THE request this verdict is about
                        // instead of matching it to the address
                        .uuid("sagaId")
                        .stringType("email", "leaver@example.com")
                        // ADMIN, because an administrator's is the only closure that states
                        // choices at all — security drops a leaver's before the fact is built
                        .stringValue("initiatedBy", "ADMIN")
                        .object("policy")
                        .stringType("memes", "KEEP_POPULAR_ANONYMIZED:100")
                        .stringType("comments", "ANONYMIZE_AUTHOR")
                        .closeObject())
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "deletionRequested")
    void theFactOpensTheSagaAndCommandsThePurge(List<Message> messages) {
        RouterFixture fixture = router();
        List<EventsRouter.Outgoing> out =
                fixture.router.handle(Source.SECURITY, messages.get(0).contentsAsString());
        assertEquals(1, out.size());
        assertEquals(Destination.PARTICIPANTS, out.get(0).destination());
        assertTrue(out.get(0).payload().contains("\"PURGE_USER_CONTENT\""));
        assertTrue(out.get(0).payload().contains("\"initiatedBy\":\"SELF\""),
                "the basis rides every command, not only the closure: " + out.get(0).payload());
    }

    @Test
    @PactTestFor(pactMethod = "deletionRequestedWithPolicy")
    void theLeaversChoicesRideTheCommand(List<Message> messages) {
        RouterFixture fixture = router();
        List<EventsRouter.Outgoing> out =
                fixture.router.handle(Source.SECURITY, messages.get(0).contentsAsString());
        assertEquals(1, out.size());
        assertTrue(out.get(0).payload().contains("\"policy\""), "the choices must be ferried");
        assertTrue(out.get(0).payload().contains("\"initiatedBy\":\"ADMIN\""),
                "and so must the basis that licenses them: " + out.get(0).payload());
    }
}
