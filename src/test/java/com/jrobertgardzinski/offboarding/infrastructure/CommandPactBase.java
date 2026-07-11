package com.jrobertgardzinski.offboarding.infrastructure;

import java.util.UUID;

/**
 * The REAL command-producing path, shared by the provider tests: a deletion fact goes through the
 * router and the {@code PURGE_USER_CONTENT} it answers with is the payload the participants'
 * pacts are verified against — never hand-written JSON.
 */
public final class CommandPactBase {

    private CommandPactBase() {
    }

    public static String realPurgeCommand() {
        return RouterFixture.router().router.handle(RouterFixture.FACTS_TOPIC,
                        "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                                + "\"email\":\"leaver@example.com\",\"version\":1}")
                .get(0).payload();
    }

    public static String realPurgeCommandWithPolicy() {
        return RouterFixture.router().router.handle(RouterFixture.FACTS_TOPIC,
                        "{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"ACCOUNT_DELETION_REQUESTED\","
                                + "\"email\":\"leaver@example.com\","
                                + "\"policy\":{\"memes\":\"KEEP_POPULAR_ANONYMIZED:5\",\"comments\":\"DELETE\"},"
                                + "\"version\":1}")
                .get(0).payload();
    }
}
