-- The facts that JOINED a saga instead of opening one. V1 gave offboarding_sagas a UNIQUE fact_id
-- so a replayed deletion fact finds its saga rather than forking a new one — but only the fact the
-- saga was OPENED with is written there. A second fact for the same account (a second request, or
-- a redelivery arriving while the first saga still ran) was answered with the running saga's id and
-- then forgotten entirely.
--
-- That silence outlives the saga. running_email goes NULL the moment the case finishes, so once the
-- joined saga completed or compensated, BOTH lookups missed the joining fact: a redelivery of it —
-- which is routine, because the pass that handled it may have failed its flush and been rewound —
-- opened a BRAND NEW saga and commanded the whole purge again. For an account whose failed case had
-- just been compensated (content restored, security told the purge failed, the account unlocked),
-- that second saga marks, confirms, completes and ERASES the content of an account that is alive
-- and well.
--
-- One row per joining fact, so every fact that was ever answered with a saga id can find that saga
-- again. Opening facts stay where they are (offboarding_sagas.fact_id, still UNIQUE — it is also
-- the constraint that decides the race between two facts opening at once); the lookup reads both.
CREATE TABLE offboarding_saga_facts (
    fact_id UUID PRIMARY KEY,
    saga_id UUID NOT NULL REFERENCES offboarding_sagas (id)
);
-- the retention window deletes these by SAGA, exactly as it does the confirmations
CREATE INDEX idx_offboarding_saga_facts_saga ON offboarding_saga_facts (saga_id);
