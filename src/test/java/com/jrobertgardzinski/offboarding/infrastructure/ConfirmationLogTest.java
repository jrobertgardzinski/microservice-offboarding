package com.jrobertgardzinski.offboarding.infrastructure;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static com.jrobertgardzinski.offboarding.infrastructure.RouterFixture.router;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the router SAYS about a confirmation, which for a stray one was wrong: both a recorded
 * confirmation and a dropped one produced "recorded ... purge confirmation; saga not complete yet".
 * An operator chasing a deletion that never completes reads that line as "we have it, we are
 * waiting for the others" — while the confirmation had in fact been thrown away, because no saga
 * was waiting for it (a closed case, a mangled echo, an account with no deletion under way). The
 * two events need two lines. Logback is already this service's logger, so the assertion needs no
 * new dependency, only an appender on the router's own logger.
 */
class ConfirmationLogTest {

    private static final String LEAVER = "leaver@example.com";

    private final Logger routerLog = (Logger) LoggerFactory.getLogger(EventsRouter.class);
    private final ListAppender<ILoggingEvent> lines = new ListAppender<>();

    @BeforeEach
    void captureTheRoutersLog() {
        lines.start();
        routerLog.addAppender(lines);
    }

    @AfterEach
    void releaseTheRoutersLog() {
        routerLog.detachAppender(lines);
        lines.stop();
    }

    @Test
    void a_recorded_confirmation_says_it_was_recorded() {
        RouterFixture fixture = router().withRunningSagaFor(LEAVER);
        fixture.router.handle("memes-events", confirmation(fixture.store.all().get(0).id));
        assertTrue(logged("recorded memes purge confirmation"),
                "a confirmation that landed on a saga is progress and must read like it: " + logged());
    }

    @Test
    void a_stray_confirmation_says_it_was_dropped_and_never_that_it_was_recorded() {
        // no saga at all for this address: the confirmation lands nowhere
        RouterFixture fixture = router();
        fixture.router.handle("memes-events", confirmation(UUID.randomUUID()));
        assertTrue(logged("dropping stray memes purge confirmation"),
                "a dropped confirmation must say so: " + logged());
        assertFalse(logged("recorded memes purge confirmation"),
                "and must never claim it was recorded — nothing was: " + logged());
    }

    @Test
    void an_echo_of_a_closed_case_is_a_stray_too() {
        // the case that made this line dangerous: a late confirmation of a saga the portal already
        // gave up on. It changes nothing, and the log must not suggest the saga is still collecting
        RouterFixture fixture = router().withRunningSagaFor(LEAVER);
        UUID closed = fixture.store.all().get(0).id;
        fixture.store.complete(LEAVER, java.time.Instant.parse("2026-07-11T12:00:00Z"));
        fixture.router.handle("memes-events", confirmation(closed));
        assertTrue(logged("dropping stray memes purge confirmation"), logged().toString());
        assertFalse(logged("recorded memes purge confirmation"), logged().toString());
    }

    private boolean logged(String fragment) {
        return lines.list.stream().anyMatch(event -> event.getFormattedMessage().contains(fragment));
    }

    private List<String> logged() {
        return lines.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private static String confirmation(UUID sagaId) {
        return "{\"type\":\"USER_CONTENT_PURGED\",\"sagaId\":\"" + sagaId + "\",\"email\":\""
                + LEAVER + "\",\"version\":1}";
    }
}
