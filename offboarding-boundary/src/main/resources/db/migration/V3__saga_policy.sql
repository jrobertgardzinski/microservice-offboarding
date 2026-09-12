-- The leaver's policy choices, persisted beside the saga. Until now the choices only rode the
-- FIRST purge command, straight off the deletion fact: the fact itself was never stored, so a
-- re-commanded purge (the sweeper's retry) went out bare and the participants fell back to their
-- defaults — the leaver's explicit wishes silently lost on the unluckiest path. The policy is an
-- opaque JSON object ferried verbatim (its vocabulary belongs to the content services, exactly as
-- on the wire), stored as TEXT and NULL for the sagas whose fact carried no choices — including
-- every saga from before this migration, for which bare retries remain the honest truth.
ALTER TABLE offboarding_sagas ADD COLUMN policy TEXT;
