-- The whole schema, in one file: nothing is deployed anywhere, so there is no history to replay.
-- A change edits this file; a running dev database is recreated (docker compose -p security down -v).

CREATE TABLE offboarding_sagas (
    id                    UUID PRIMARY KEY,
    fact_id               UUID         NOT NULL UNIQUE,
    email                 VARCHAR(255) NOT NULL,
    state                 VARCHAR(20)  NOT NULL,
    created_at            TIMESTAMP    NOT NULL,
    updated_at            TIMESTAMP    NOT NULL,
    outcome_announced     BOOLEAN      NOT NULL DEFAULT FALSE,
    -- the address while the saga runs, null once it is settled: one running saga per address
    running_email         VARCHAR(255),
    retries               INT          NOT NULL DEFAULT 0,
    policy                TEXT,
    security_saga_id      UUID,
    required_participants TEXT,
    initiated_by          TEXT,
    CONSTRAINT uq_offboarding_running_email UNIQUE (running_email)
);
CREATE INDEX idx_offboarding_state_created ON offboarding_sagas (state, created_at);
CREATE INDEX idx_offboarding_state_updated ON offboarding_sagas (state, updated_at);

CREATE TABLE offboarding_confirmations (
    saga_id      UUID        NOT NULL REFERENCES offboarding_sagas (id),
    participant  VARCHAR(50) NOT NULL,
    confirmed_at TIMESTAMP   NOT NULL,
    PRIMARY KEY (saga_id, participant)
);

-- a fact that joined a running saga instead of opening a new one
CREATE TABLE offboarding_saga_facts (
    fact_id UUID PRIMARY KEY,
    saga_id UUID NOT NULL REFERENCES offboarding_sagas (id)
);
CREATE INDEX idx_offboarding_saga_facts_saga ON offboarding_saga_facts (saga_id);
