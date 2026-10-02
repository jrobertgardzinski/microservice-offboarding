package com.jrobertgardzinski.offboarding.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What {@link SagaStore} promises, asked of EVERY implementation — the JDBC adapter the service
 * runs on and the in-memory double used in its place.
 *
 * <p>The double is not a convenience here: {@code portal-specs} builds the whole portal on it, and
 * its races layer snapshots it, rolls it back and reads its outbox query. Every promise those
 * depend on was, until now, kept by two classes that only agreed because somebody kept writing
 * "mirrors the JDBC adapter" in a comment. This is that comment, executable.
 *
 * <p>What it does NOT cover, on purpose: the adapter's own schema — the V2 UNIQUE constraints under
 * real thread races, the migrations, the once-latch as an UPDATE … WHERE state = 'STARTED'. Those
 * belong to {@code JdbcSagaStoreTest}, which keeps them, because a contract test can only ask what
 * both sides can answer.
 *
 * <p>Fresh ids per test method: one implementation is a database other suites share.
 */
public abstract class SagaStoreContractTest {

    protected abstract SagaStore store();

    private static final Set<String> THREE = Set.of("memes", "comments", "collections");

    private static final Instant T0 = Instant.parse("2026-09-29T12:00:00Z");

    /** Past every deadline this contract sets, so a sweep sees whatever is still STARTED. */
    private static final Instant LATER = T0.plus(Duration.ofHours(1));

    private final String run = UUID.randomUUID().toString().substring(0, 8);

    private String anAddress(String who) {
        return who + "-" + run + "@example.com";
    }

    /** The id identity minted for an address — the tests speak addresses, the store ids. */
    private static UUID idOf(String email) {
        return UUID.nameUUIDFromBytes(("user:" + email).getBytes());
    }

    private UUID aCaseFor(String email) {
        return store().start(UUID.randomUUID(), email, idOf(email), T0);
    }

    private Recorded confirmed(UUID saga, String email, String participant) {
        Optional<Recorded> recorded = store().confirm(idOf(email), saga, participant, THREE, T0);
        assertTrue(recorded.isPresent(), participant + "'s confirmation landed nowhere");
        return recorded.get();
    }

    private List<Retry> retriesIn(SweepResult swept) {
        return swept.retries();
    }

    // ---- opening a case ----------------------------------------------------------------------

    @Test
    @DisplayName("the same fact twice answers with the same case, and starting it again opens nothing")
    void a_replayed_fact_answers_with_the_case_it_opened() {
        String alice = anAddress("alice");
        UUID fact = UUID.randomUUID();

        UUID first = store().start(fact, alice, idOf(alice), T0);
        UUID again = store().start(fact, alice, idOf(alice), T0.plusSeconds(5));

        assertEquals(first, again, "a redelivered fact forked a second case for one request");
    }

    @Test
    @DisplayName("a replayed fact still answers with its own case after that case has finished")
    void a_replayed_fact_answers_after_completion() {
        String alice = anAddress("alice");
        UUID fact = UUID.randomUUID();
        UUID saga = store().start(fact, alice, idOf(alice), T0);
        assertTrue(store().complete(saga, T0), "a case with nobody to wait for did not settle");

        assertEquals(saga, store().start(fact, alice, idOf(alice), LATER),
                "the fact's claim on its case was forgotten the moment the case ended, so a "
                        + "redelivery would open a second deletion for one request");
    }

    @Test
    @DisplayName("a second request while one is running joins the case that is running")
    void a_second_request_joins_the_running_case() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);

        UUID second = store().start(UUID.randomUUID(), alice, idOf(alice), T0.plusSeconds(30));

        assertEquals(saga, second, "one account ended up with two deletions in flight");
    }

    @Test
    @DisplayName("a new fact for an account with nothing running opens a case of its own")
    void a_quiet_account_gets_a_fresh_case() {
        String alice = anAddress("alice");
        UUID first = aCaseFor(alice);
        assertTrue(store().complete(first, T0));

        UUID second = store().start(UUID.randomUUID(), alice, idOf(alice), LATER);

        assertFalse(first.equals(second), "asking again after the last case closed changed nothing");
    }

    // ---- confirmations ----------------------------------------------------------------------

    @Test
    @DisplayName("the last required participant is the one whose confirmation completes the case")
    void the_last_confirmation_completes() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);

        assertFalse(confirmed(saga, alice, "memes").completedSaga());
        assertFalse(confirmed(saga, alice, "comments").completedSaga());
        assertTrue(confirmed(saga, alice, "collections").completedSaga(),
                "every part had spoken and the case was still waiting for somebody");
    }

    @Test
    @DisplayName("the same confirmation twice completes once, and the second is not the one that did it")
    void a_duplicate_confirmation_completes_once() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);
        confirmed(saga, alice, "memes");
        confirmed(saga, alice, "comments");
        assertTrue(confirmed(saga, alice, "collections").completedSaga());

        Optional<Recorded> again = store().confirm(idOf(alice), saga, "collections", THREE, LATER);

        assertTrue(again.isEmpty() || !again.get().completedSaga(),
                "a redelivered confirmation completed a case that was already complete, so "
                        + "whatever the caller does on completion would happen twice");
    }

    @Test
    @DisplayName("a confirmation with no case to land on is a stray and records nothing")
    void a_stray_confirmation_records_nothing() {
        String alice = anAddress("alice");

        assertTrue(store().confirm(idOf(alice), UUID.randomUUID(), "memes", THREE, T0).isEmpty(),
                "a confirmation naming a case nobody opened was recorded somewhere");
    }

    @Test
    @DisplayName("a confirmation naming a case that has closed is a stray, not a fallback")
    void a_confirmation_for_a_closed_case_does_not_fall_back() {
        String alice = anAddress("alice");
        UUID closed = aCaseFor(alice);
        assertTrue(store().complete(closed, T0));
        UUID running = store().start(UUID.randomUUID(), alice, idOf(alice), LATER);

        Optional<Recorded> recorded = store().confirm(idOf(alice), closed, "memes", THREE, LATER);

        assertTrue(recorded.isEmpty() || !running.equals(recorded.get().sagaId()),
                "a confirmation echoing a finished case was credited to the one running now");
    }

    // ---- completing ------------------------------------------------------------------------

    @Test
    @DisplayName("completing is a once-latch: exactly one call learns it happened")
    void completing_is_a_once_latch() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);

        assertTrue(store().complete(saga, T0));
        assertFalse(store().complete(saga, T0.plusSeconds(1)),
                "two callers both believed they were the one that finished the case");
    }

    @Test
    @DisplayName("a case that recorded who must confirm is never completed for having nobody to wait for")
    void a_recorded_quorum_is_not_completed_away() {
        String alice = anAddress("alice");
        UUID saga = store().start(new Opening(UUID.randomUUID(), alice, null, null, THREE, null,
                idOf(alice)), T0);

        assertFalse(store().complete(saga, T0),
                "a case with three participants on the row was settled as having none, which "
                        + "would announce the portal purged with no confirmation on file");
    }

    // ---- the sweeper ------------------------------------------------------------------------

    @Test
    @DisplayName("an untouched case comes back as a candidate, and sweeping twice charges nothing")
    void sweeping_hands_back_candidates_without_charging() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);

        List<Retry> first = retriesIn(store().sweepOverdue(LATER, 2, LATER));
        List<Retry> second = retriesIn(store().sweepOverdue(LATER, 2, LATER));

        assertEquals(List.of(0), first.stream().filter(retry -> retry.sagaId().equals(saga))
                .map(Retry::retriesSoFar).toList());
        assertEquals(List.of(0), second.stream().filter(retry -> retry.sagaId().equals(saga))
                .map(Retry::retriesSoFar).toList(),
                "the second sweep found the case on a later round, so merely selecting it spent "
                        + "one of its retries and a dead broker could burn them all");
    }

    @Test
    @DisplayName("a retry is charged for the round the sweep handed out, and charged once")
    void a_retry_is_charged_once_for_the_round_it_was_offered() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);
        assertEquals(1, retriesIn(store().sweepOverdue(LATER, 2, LATER)).stream()
                .filter(retry -> retry.sagaId().equals(saga)).count());

        assertTrue(store().retryDelivered(saga, 0, LATER), "the charge did not land");
        assertFalse(store().retryDelivered(saga, 0, LATER),
                "two sweepers reporting the same re-command both charged it, so the case would "
                        + "spend two of its retries on one attempt");
    }

    @Test
    @DisplayName("the charge also restarts the overdue clock, so the part gets its whole budget")
    void a_charge_restarts_the_clock() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);
        Instant asked = T0.plusSeconds(120);
        assertTrue(store().retryDelivered(saga, 0, asked));

        List<Retry> tooSoon = retriesIn(store().sweepOverdue(asked, 2, asked));

        assertEquals(List.of(), tooSoon.stream().filter(retry -> retry.sagaId().equals(saga))
                        .toList(),
                "the case was overdue again the instant it was re-commanded, so the participant "
                        + "never had the timeout it was given");
    }

    @Test
    @DisplayName("retries exhausted means compensated, with whatever had confirmed by then")
    void exhausted_retries_compensate() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);
        confirmed(saga, alice, "memes");

        List<Compensated> given = store().sweepOverdue(LATER, 0, LATER).compensated().stream()
                .filter(one -> one.sagaId().equals(saga)).toList();

        assertEquals(1, given.size(), "a case with no retries left was swept and not given up on");
        assertEquals(Set.of("memes"), given.get(0).confirmed(),
                "the capitulation forgot which parts had already reserved something, and that is "
                        + "the set the compensation has to address");
    }

    @Test
    @DisplayName("a case given up on is not swept again")
    void a_compensated_case_is_done() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);
        assertEquals(1, store().sweepOverdue(LATER, 0, LATER).compensated().stream()
                .filter(one -> one.sagaId().equals(saga)).count());

        SweepResult again = store().sweepOverdue(LATER.plusSeconds(60), 0, LATER.plusSeconds(60));

        assertEquals(List.of(), again.compensated().stream()
                        .filter(one -> one.sagaId().equals(saga)).toList(),
                "the same case was given up on twice, which is two verdicts for one deletion");
    }

    // ---- the outbox half -------------------------------------------------------------------

    @Test
    @DisplayName("a finished case owes its outcome until somebody says it was announced")
    void a_finished_case_owes_its_outcome() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);
        assertTrue(store().complete(saga, T0));

        assertTrue(owing(saga, LATER), "a finished case owed nothing, so nothing would ever say it");
        store().markAnnounced(saga);
        assertFalse(owing(saga, LATER), "the outcome was announced and the case still owes it, so "
                + "the next sweep would announce it again");
    }

    @Test
    @DisplayName("an outcome is not owed while it is merely in flight")
    void the_age_guard_holds() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);
        assertTrue(store().complete(saga, T0));

        assertFalse(owing(saga, T0), "a case that finished this very instant was already counted "
                + "as owing, which double-publishes every outcome the caller is still sending");
    }

    @Test
    @DisplayName("a case still running owes nothing at all")
    void a_running_case_owes_nothing() {
        String alice = anAddress("alice");
        UUID saga = aCaseFor(alice);

        assertFalse(owing(saga, LATER), "a case that has decided nothing was asked to announce it");
    }

    private boolean owing(UUID saga, Instant olderThan) {
        return store().unannouncedOutcomes(olderThan).stream()
                .map(PendingOutcome::sagaId)
                .anyMatch(saga::equals);
    }

    // ---- retention -------------------------------------------------------------------------

    @Test
    @DisplayName("retention drops what is finished and announced, and keeps everything else")
    void retention_drops_only_what_is_done() {
        String alice = anAddress("alice");
        String bob = anAddress("bob");
        UUID done = aCaseFor(alice);
        assertTrue(store().complete(done, T0));
        store().markAnnounced(done);
        UUID running = aCaseFor(bob);

        int dropped = store().deleteFinishedBefore(LATER);

        assertTrue(dropped >= 1, "a finished and announced case was kept for ever");
        assertTrue(store().confirm(idOf(bob), running, "memes", THREE, LATER).isPresent(),
                "retention took a case that was still running");
    }
}
