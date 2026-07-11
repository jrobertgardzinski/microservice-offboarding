# microservice-offboarding

The portal's **process manager** for account deletion — the saga orchestration that used to live
inside `microservice-security`, extracted so identity stays reusable (the two-products verdict:
the portal cleans up after itself; identity only needs to know *whether* the portal is clean).

## What it does

```
security ──ACCOUNT_DELETION_REQUESTED (security-events)──▶ offboarding
offboarding ──PURGE_USER_CONTENT (content-commands)──▶ memes / comments / user-collections
memes / comments / user-collections ──USER_CONTENT_PURGED (their topics)──▶ offboarding
offboarding ──PORTAL_CONTENT_PURGED | PORTAL_PURGE_FAILED (offboarding-events)──▶ security
```

Security announces the FACT that an account requested deletion. This service opens a saga,
commands every configured content participant to purge the leaver's content (ferrying the
leaver's policy choices untouched), collects the confirmations, and announces the **single
outcome** security waits for. No confirmation in time → the saga compensates and the failure is
announced (security unlocks the account and apologises).

**Participants are configuration, not code** — the whole point of the extraction:

```
OFFBOARDING_PARTICIPANTS=memes=memes-events,comments=comments-events,collections=usercollections-events
```

A new content service joins the saga by adding a `name=confirmation-topic` pair; an identity-only
deployment simply doesn't run this service at all (security's `account-deletion.await-portal-purge=false`
deletes immediately).

## Architecture

The seventh portal service, same flavour as `microservice-user-collections` (Helidon 4 SE on
virtual threads) because the interesting part here is the **pattern**, not a new framework:

- **application** — `BeginOffboarding`, `RecordConfirmation`, `SweepOverdue` over the `SagaStore`
  port. Framework-free.
- **infrastructure** — `EventsRouter` (the pure switchboard the scenarios and pacts drive),
  `KafkaLoop` (the real transport: consume, route, publish with the correlation-id header,
  commit), `JdbcSagaStore` (Postgres / H2-PG-mode, Flyway; confirmations are ROWS, never
  per-participant columns), `/health` + `/metrics` over HTTP.

Idempotence is the law (workspace ADR 0006, enforced by the generic `IdempotentCommandsTest`):
a replayed deletion fact finds its saga by the fact's `id` even after completion; a duplicate
confirmation is a no-op; the STARTED→COMPLETED update is a once-latch, so completion is announced
by exactly one delivery.

## Always-on by choice, scale-to-zero-ready by design

This is a long-running consumer, and that is deliberate. "Listening" costs next to nothing —
`poll()` blocks idle (~0% CPU, ~80 MB JVM) and Kafka holds the group's offsets, so downtime
delays deletions rather than losing them. The service already meets every prerequisite for
event-driven autoscaling (KEDA 0↔N on consumer-group lag): stateless process (saga state in
Postgres), idempotent handling (cold starts and redeliveries are safe), offsets at the broker.

It is not scaled to zero anyway, for three reasons: (1) on this stack's deployment target
(Compose → VPS) an idle JVM saves nothing, while an autoscaler adds a failure mode to a
GDPR-critical path; (2) the timeout sweeper must fire precisely when NO events arrive — no
events means no lag, so a lag-based scaler would never wake the one job that compensates
overdue sagas (you'd need a second cron trigger, or `minReplicas: 1`, which defeats the point);
(3) one saga type does not earn a cluster. The calculus flips when purges get heavy (scale
1→N on lag) or on per-second-billed infrastructure.

## Contracts (Pact, file mode — workspace ADR 0003)

As a **consumer** this service pins (committed in `pacts/`): security's deletion fact (id, email,
optional policy) and each participant's purge confirmation (type, email). As a **provider** it is
verified against the participants' command pacts and security's outcome pact — from sibling
checkouts; skipped, not failed, when a sibling is absent.

## Run & test

```bash
mvn test                 # scenarios (features/offboarding.feature), the law, JDBC, pacts
mvn package && java -jar target/microservice-offboarding.jar
```

Env: `OFFBOARDING_PORT` (8094), `OFFBOARDING_FACTS_TOPIC` (security-events),
`OFFBOARDING_PARTICIPANTS` (see above), `OFFBOARDING_PURGE_TIMEOUT_SEC` (120),
`KAFKA_BOOTSTRAP_SERVERS` (absent = the loop never runs), `DB_URL`/`DB_USER`/`DB_PASSWORD`
(absent = in-memory H2).

Part of a [portfolio of microservices](https://github.com/jrobertgardzinski); the deployment and
the C4 diagrams live in the workspace repo.
