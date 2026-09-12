-- The portal-side account-deletion saga, extracted from microservice-security: a saga STARTS
-- when security announces the deletion fact, collects one confirmation row per content
-- participant, and COMPLETES when every REQUIRED participant confirmed (the required set is
-- CONFIGURATION, not columns — the lesson of security's memes_purged/comments_purged/...).
-- No confirmation in time turns it COMPENSATED and the portal announces the failure.

CREATE TABLE offboarding_sagas (
    id         UUID PRIMARY KEY,
    -- the id of the ACCOUNT_DELETION_REQUESTED fact that opened this saga: at-least-once delivery
    -- may replay the fact even after the saga completed, and this is what makes the replay find
    -- its saga instead of forking a new one
    fact_id    UUID         NOT NULL UNIQUE,
    email      VARCHAR(255) NOT NULL,
    state      VARCHAR(20)  NOT NULL,
    created_at TIMESTAMP    NOT NULL,
    updated_at TIMESTAMP    NOT NULL
);
-- a plain index (H2, PG-mode, has no partial indexes; the dev profile runs these same migrations)
CREATE INDEX idx_offboarding_state_created ON offboarding_sagas (state, created_at);

CREATE TABLE offboarding_confirmations (
    saga_id      UUID        NOT NULL REFERENCES offboarding_sagas (id),
    participant  VARCHAR(50) NOT NULL,
    confirmed_at TIMESTAMP   NOT NULL,
    PRIMARY KEY (saga_id, participant)
);
