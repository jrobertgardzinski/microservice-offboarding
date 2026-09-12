-- Hardening after the resilience review: the saga table learns three things it could not
-- say before.

-- outcome_announced: the mini-outbox. Flipping to COMPLETED/COMPENSATED no longer implies the
-- outcome reached Kafka — the loop marks a saga announced only AFTER a successful producer
-- flush, and the sweeper re-publishes whatever stayed unannounced for too long. Sagas finished
-- before this migration were announced under the old fire-and-forget regime; grandfather them
-- so the sweeper does not replay history. (This runs BEFORE the dedup below on purpose: the
-- sagas the dedup compensates must NOT be grandfathered — their outcome really never went out.)
ALTER TABLE offboarding_sagas ADD COLUMN outcome_announced BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE offboarding_sagas SET outcome_announced = TRUE WHERE state IN ('COMPLETED', 'COMPENSATED');

-- running_email: "one running saga per email" was only ever an application-level
-- read-then-insert — two facts racing for the same account could fork two sagas. H2 (PG mode)
-- has no partial indexes (see V1), so instead of Postgres' WHERE state='STARTED' index the
-- invariant lives in a nullable column: running_email carries the email while the saga runs
-- and goes NULL in the very UPDATE that flips the state off STARTED. Both Postgres and H2
-- keep NULLs out of UNIQUE, so finished sagas never collide.
ALTER TABLE offboarding_sagas ADD COLUMN running_email VARCHAR(255);

-- Dedup BEFORE the constraint: the old read-then-insert race may already have forked several
-- STARTED sagas for one email, and adding UNIQUE(running_email) over that dirt would fail the
-- whole migration. Keep the NEWEST started saga per email running (created_at, id as the
-- deterministic tie-break) and close the older forks as COMPENSATED — they were never
-- completed, so compensating an abandoned duplicate is safe, and their running_email stays
-- NULL so they cannot collide. Their outcome_announced deliberately stays FALSE: the sweeper
-- will announce PORTAL_PURGE_FAILED for them, which is the correct outcome for a saga the
-- portal abandoned.
UPDATE offboarding_sagas older SET state = 'COMPENSATED'
WHERE older.state = 'STARTED'
  AND EXISTS (SELECT 1 FROM offboarding_sagas newer
              WHERE newer.email = older.email
                AND newer.state = 'STARTED'
                AND (newer.created_at > older.created_at
                     OR (newer.created_at = older.created_at AND newer.id > older.id)));

UPDATE offboarding_sagas SET running_email = email WHERE state = 'STARTED';
ALTER TABLE offboarding_sagas ADD CONSTRAINT uq_offboarding_running_email UNIQUE (running_email);

-- retries: how many times the sweeper re-commanded the purge past the deadline. Participants
-- are idempotent, so re-commanding is free — only when the retries are exhausted may a saga
-- give up and compensate.
ALTER TABLE offboarding_sagas ADD COLUMN retries INT NOT NULL DEFAULT 0;
