# microservice-offboarding — Test Report & Documentation

Generated from Allure results by `build_documentation.py` on 2026-08-11. Behaviors below are **verified by passing tests** — rerun the suite, rerun this script, and the document cannot drift from the code.

## 📊 Execution Summary

| Module | Total | Passed | Failed | Broken | Skipped | Duration |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| microservice-offboarding | 148 | 148 | 0 | 0 | 0 | 71.02s |

## 📝 Test Documentation (Behaviors)

This section describes the verified system behaviors based on passing tests.

### Epic: Config

#### Feature: Boot-time validation

- **a_mangled_numeric_env_refuses_to_boot_naming_the_variable()**
- **a_non_positive_timeout_refuses_to_boot_without_reciting_long_max()**
- **a_parsable_numeric_env_wins_over_the_default()**
- **a_participant_entry_that_is_not_name_equals_topic_refuses_to_boot()**
- **a_port_outside_1_to_65535_refuses_to_boot_naming_variable_and_value()**
- **a_repeated_TOPIC_refuses_to_boot_instead_of_dropping_a_participant()**
- **a_repeated_participant_NAME_refuses_to_boot_instead_of_shrinking_the_quorum()**
- **a_retry_budget_outside_0_to_100_refuses_to_boot()**
- **a_stall_at_or_above_the_interval_is_kept()**
- **a_stall_below_the_sweep_interval_is_floored_to_the_interval()**
- **a_value_inside_its_range_passes_through()**
- **an_absent_or_blank_numeric_env_falls_back_to_the_default()**
- **an_alive_stall_at_or_above_the_derived_floor_is_kept()**
- **an_alive_stall_below_the_derived_floor_is_floored()**
- **one_participant_may_be_named_after_another_participants_topic_prefix()**
- **the_alive_floor_covers_the_whole_worst_legal_iteration()**
- **the_code_default_sits_above_the_floor_instead_of_being_corrected_by_it()**
- **the_consumers_own_blocking_clock_is_explicit_and_inside_the_alive_floor()**
- **the_database_clocks_are_a_term_of_the_floor_not_an_unbounded_wait()**
- **the_shipped_participant_spec_parses_into_one_topic_per_participant()**
- **whitespace_around_the_pairs_is_forgiven_and_an_empty_spec_stays_legal()**

### Epic: Contract

#### Feature: Deletion request

- **theFactOpensTheSagaAndCommandsThePurge(List)**
- **theLeaversChoicesRideTheCommand(List)**

#### Feature: Honest pact skips

- **every checked-out neighbour's pact is where its provider test looks for it**

#### Feature: Purge commands

##### Story: Comments

- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-comments - a purge user content command**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-comments - a purge user content command with an explicit policy**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-comments - a restore user content command compensating the saga**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-comments - an erase user content command closing the saga**

##### Story: Memes

- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-memes - a purge user content command**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-memes - a purge user content command with an explicit policy**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-memes - a restore user content command compensating the saga**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-memes - an erase user content command closing the saga**

##### Story: User collections

- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-user-collections - a purge user content command**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-user-collections - a restore user content command compensating the saga**
- **theCommandShapeTheParticipantReliesOn(PactVerificationContext) microservice-user-collections - an erase user content command closing the saga**

#### Feature: Purge confirmations

##### Story: Comments

- **theConfirmationAdvancesTheSaga(List)**

##### Story: Memes

- **theConfirmationAdvancesTheSaga(List)**

##### Story: User collections

- **theConfirmationAdvancesTheSaga(List)**

#### Feature: Purge verdicts

- **theOutcomeShapeSecurityReliesOn(PactVerificationContext) microservice-security - a portal content purged announcement**
- **theOutcomeShapeSecurityReliesOn(PactVerificationContext) microservice-security - a portal purge failed announcement**

### Epic: Executable specs

#### Feature: Beginning the offboarding — a deletion FACT opens a CASE

- **A FACT the portal cannot place is dropped — never the requests behind it**
- **A deletion FACT commands the content PURGE**
- **A second deletion request joins the CASE already underway**
- **Past RETENTION the portal remembers nothing — a replayed FACT opens a brand-new CASE**
- **The leaver's choices ride the command untouched**
- **With no content participants the portal is instantly clean**

#### Feature: Collecting the CONFIRMATIONS — the last one settles the CASE

- **A CONFIRMATION for nobody's CASE is a stray, not an error**
- **A CONFIRMATION from a closed CASE does not touch a new one**
- **A CONFIRMATION may echo the PURGE command it answers**
- **A late CONFIRMATION cannot rewrite an announced failure**
- **An early CONFIRMATION announces nothing yet**
- **The last CONFIRMATION also commands the ERASURE — that is what closes the CASE**
- **The last CONFIRMATION announces the portal purged**

#### Feature: The SWEEP — silence has a deadline

- **An OUTCOME that reached security is not announced twice**
- **An OUTCOME the portal failed to announce is announced at the next SWEEP**
- **Giving up restores what it had reserved, and only then apologises**
- **Silence outlasting every retry announces the failure, naming the partial purge**
- **Silence past the deadline retries the PURGE command before giving up**
- **The leaver's choices survive the retry**

### Epic: Infrastructure

#### Feature: Confirmation log lines

- **a_recorded_confirmation_says_it_was_recorded()**
- **a_stray_confirmation_says_it_was_dropped_and_never_that_it_was_recorded()**
- **an_echo_of_a_closed_case_is_a_stray_too()**

#### Feature: Database timeouts

- **the_connection_timeouts_reach_hikari_and_the_driver()**
- **the_in_memory_h2_gets_hikaris_clock_but_none_of_the_postgres_properties()**
- **the_socket_timeout_reaches_the_postgres_driver_properties()**
- **the_statement_timeout_rides_along_as_a_session_option()**

#### Feature: Kafka transport

- **a_broker_outage_keeps_the_liveness_beat_inside_the_producer_clock_rhythm()**
- **a_completed_but_unannounced_outcome_is_republished_by_the_sweeper()**
- **a_dead_broker_on_a_quiet_topic_fails_the_probe_and_stalls_readiness_not_liveness()**
- **a_deletion_fact_becomes_a_command_and_all_confirmations_become_the_outcome()**
- **a_permanently_failing_pass_stalls_readiness_while_the_liveness_heartbeat_keeps_beating()**
- **a_poison_pill_is_dropped_and_a_restart_never_sees_it_again()**
- **a_recommand_from_the_sweeper_carries_a_correlation_id_minted_from_its_saga()**
- **a_rejected_outcome_send_is_never_marked_announced_and_the_sweeper_delivers_after_repair()**
- **a_stopped_loop_stalls_the_health_it_reports()**
- **an_infrastructure_outage_is_retried_until_the_record_lands_exactly_once()**
- **an_undeliverable_recommand_burns_no_retries_and_the_saga_never_compensates_prematurely()**

#### Feature: Poison pills

- **a_confirmation_echoing_the_sagas_own_id_lands_precisely()**
- **a_confirmation_with_a_blank_email_drops_and_confirms_nothing()**
- **a_confirmation_with_a_mangled_sagaId_drops_and_confirms_nothing()**
- **a_confirmation_with_an_explicit_null_sagaId_drops_and_confirms_nothing()**
- **a_confirmation_without_an_email_drops_and_confirms_nothing()**
- **a_confirmation_without_the_sagaId_field_lands_via_the_email_fallback()**
- **a_fact_with_a_mangled_id_drops_without_a_saga()**
- **a_fact_without_an_email_drops_without_a_saga()**
- **a_fact_without_an_id_drops_without_a_saga()**

#### Feature: Policy size cap

- **a_policy_within_the_cap_still_rides_and_is_stored_verbatim()**
- **an_oversized_policy_is_dropped_from_the_command_and_the_saga_alike()**
- **the_cap_counts_utf8_bytes_not_characters()**

#### Feature: Progress metrics

- **a_delivered_retry_moves_its_counter()**
- **a_failed_sweeper_pass_moves_its_counter()**
- **both_counters_are_typed_for_prometheus()**

#### Feature: Readiness probe

- **a_broker_that_really_does_not_answer_still_becomes_a_broker_verdict()**
- **a_shutdown_interrupt_rides_through_the_probe_untouched()**
- **a_shutdown_wakeup_rides_through_the_probe_untouched()**

#### Feature: Saga store

- **a_compensated_outcome_carries_the_partial_purge()**
- **a_completed_saga_never_compensates()**
- **a_confirmation_addressed_by_saga_id_lands_on_that_saga()**
- **a_confirmation_echoing_a_finished_saga_is_a_stray_and_never_touches_a_newer_one()**
- **a_delivered_retry_is_not_counted_against_a_finished_saga()**
- **a_delivered_retry_stamps_the_callers_instant_on_the_saga()**
- **a_delivery_report_for_an_unknown_saga_reports_no_charge()**
- **a_finished_saga_owes_its_outcome_until_marked_announced()**
- **a_fresh_unannounced_outcome_is_not_republished_yet()**
- **a_lengthened_participant_list_does_not_hold_an_open_saga_hostage()**
- **a_recorded_confirmation_is_distinguishable_from_a_stray()**
- **a_replayed_fact_finds_its_saga_even_after_completion()**
- **a_saga_started_without_policy_retries_without_one()**
- **a_saga_that_recorded_no_quorum_falls_back_to_the_configured_one()**
- **a_second_fact_hands_the_running_saga_the_newer_security_handle()**
- **a_second_request_while_one_runs_joins_the_running_saga()**
- **a_second_started_row_for_one_email_is_rejected_by_the_database_itself()**
- **a_stray_confirmation_records_nothing()**
- **an_empty_required_set_completes_via_complete()**
- **an_undelivered_retry_burns_nothing_and_never_capitulates()**
- **every_delivered_recommand_buys_a_whole_timeout_before_the_next_decision()**
- **only_the_last_required_confirmation_completes_and_only_once()**
- **racing_starts_for_the_same_email_agree_on_one_saga()**
- **racing_starts_with_the_same_fact_agree_on_one_saga()**
- **the_quorum_recorded_at_start_survives_a_reconfiguration()**
- **the_retention_window_deletes_finished_and_announced_sagas_with_their_confirmations()**
- **the_security_handle_is_stored_at_start_and_handed_back_with_every_verdict()**
- **the_stored_policy_rides_every_retry_candidate()**
- **the_sweep_compensates_only_the_overdue_and_only_once()**

#### Feature: Schema migrations

##### Story: V2 dedup of forked sagas

- **the_constraint_added_over_deduplicated_data_still_bites()**
- **v2_survives_forked_started_sagas_and_keeps_only_the_newest_running()**
- **v2_ties_on_created_at_break_deterministically_by_id()**

### Epic: Saga

#### Feature: Cascade traffic isolation

- **a COMMENTS_DELETED on the confirmations' topic confirms nothing and says nothing**
- **a whole cascade cannot close a saga that only one participant has confirmed**
- **and the cascade's traffic does not get in the confirmation's way**
- **nor does the hop before it, MEME_DELETED on the memes participant's topic**

#### Feature: Outcome outbox

- **a_fresh_unannounced_outcome_waits_out_the_age_guard()**
- **a_marked_outcome_is_never_republished()**
- **an_unannounced_completion_is_republished_by_the_sweep()**
- **the_instantly_clean_outcome_rides_the_same_outbox()**

#### Feature: Verdict correlation

- **a_fact_with_a_mangled_handle_is_a_poison_pill()**
- **a_fact_without_a_handle_still_opens_a_saga_and_its_verdict_carries_none()**
- **the_completion_verdict_echoes_the_handle_from_the_fact()**
- **the_failure_verdict_and_its_republication_echo_the_handle_too()**
- **the_instantly_clean_verdict_echoes_the_handle_and_is_announced_once()**

### Epic: Use case

#### Feature: Idempotent commands

- **begin a fresh offboarding**
- **begin over an already-running saga**
- **begin with no participants (instant completion)**
- **record a first confirmation**
- **record a stray confirmation (no saga)**
- **record an already-recorded confirmation**
- **record the completing confirmation**
- **sweep the overdue**

