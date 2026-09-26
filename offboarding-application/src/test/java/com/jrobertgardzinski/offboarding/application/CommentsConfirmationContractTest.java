package com.jrobertgardzinski.offboarding.application;

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
import io.qameta.allure.Story;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static com.jrobertgardzinski.offboarding.application.RouterFixture.router;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The comments twin of {@link MemesConfirmationContractTest} — same fields, its own topic. */
@Epic("Contract")
@Feature("Purge confirmations")
@Story("Comments")
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "microservice-comments", providerType = ProviderType.ASYNCH,
        pactVersion = PactSpecVersion.V3)
class CommentsConfirmationContractTest {

    @Pact(consumer = "microservice-offboarding")
    MessagePact purgeConfirmation(MessagePactBuilder builder) {
        return builder.expectsToReceive("a user content purged confirmation")
                .withContent(new PactDslJsonBody()
                        .stringValue("type", "USER_CONTENT_PURGED")
                        .uuid("userId")
                        // the saga id the purge command carried, echoed back — how a confirmation
                        // addresses its saga without leaning on the email
                        .uuid("sagaId"))
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "purgeConfirmation")
    void theConfirmationAdvancesTheSaga(List<Message> messages) throws Exception {
        String payload = messages.get(0).contentsAsString();
        // the confirmation echoes the id of ITS saga — seed the saga under exactly that id (an
        // echoed id matching no running saga is a stray by design)
        RouterFixture fixture = router()
                .withRunningSaga(RouterFixture.sagaIdOf(payload), "leaver@example.com");
        fixture.router.handle(Source.participant("comments"), payload);
        assertTrue(fixture.store.all().get(0).confirmed.contains("comments"),
                "the comments confirmation must be recorded against the running saga");
    }
}
