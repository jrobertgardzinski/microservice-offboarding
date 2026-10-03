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
virtual threads) because the interesting part here is the **pattern**, not a new framework.

**The estate's layers, as Maven modules.** The names are the estate's own, and that matters more
than it looks: **a monolith is assembled by taking every service's modules up to and including
`application`** and supplying one infrastructure underneath — a service that called its layers
something else would not slot in. (This one skips `config`: its numbers turned out to be constants
with a written rationale rather than dials, see `SweepOverdue`, so an empty module would be
ceremony.) Modules rather than packages because the split had to *hold*: the saga's Gherkin
scenarios used to import the switchboard from the transport package, so the spec of the process
could not be read without reading Kafka. A package cannot prevent that; a missing dependency can.

- **offboarding-domain** — the saga's data (`Opening`, `Recorded`, `Retry`, `Compensated`,
  `SweepResult`, `PendingOutcome`), the facts this service states about itself (`Observation`) and
  the `SagaStore` port the saga is persisted through, with its fake and its contract test beside
  it. No transport, no framework.
- **offboarding-system** — the use cases, one per door into the process manager:
  `BeginOffboarding`, `RecordConfirmation`, `SweepOverdue`, working over that port.
- **offboarding-application** — the orchestrator: `EventsRouter`, the switchboard between those
  use cases, talking in `Source` and `Destination` and knowing no transport at all. The scenarios
  and every contract with a neighbour live here, because this is the layer that owns the
  conversation.
- **offboarding-infrastructure** — the deployable, and everything with the outside in it:
  `KafkaLoop` (consume, route, publish with the correlation-id header, commit), `SagaTopics`
  (the only class that knows a topic name — swapping it for an in-memory bus is what "the same
  saga in a monolith" means in practice), `JdbcSagaStore`
  (Postgres / H2-PG-mode, Flyway; confirmations are ROWS, never per-participant columns),
  `/health` + `/metrics` over HTTP, and `Main` — the composition root that decides which adapter
  each port gets. It keeps the artifact name `microservice-offboarding.jar`, so the Dockerfile,
  the compose build and the CI boot smoke only had to learn one new path.

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

As a **consumer** this service pins (committed in `pacts/`): security's deletion fact
(`sagaId`, `id`, `userId`, `email`, `initiatedBy`, policy optional) and each participant's purge
confirmation (`sagaId`, `type`, `userId`). The leaver in both is the `userId` — a confirmation
carries no address at all, and the fact's `email` rides along only because the farewell mail and
security's own verdict need something to write to. As a **provider** it is verified against the
participants' command pacts and security's outcome pact — from sibling checkouts; skipped, not
failed, when a sibling is absent.

## Run & test

```bash
mvn test                 # the specs/ scenarios (begin, record, sweep), the law, JDBC, pacts
mvn package && java -jar target/microservice-offboarding.jar
```

Env: `OFFBOARDING_PROFILE` (`dev`|`test`|`prod`, **required** — a start that names no profile
is refused by name, see `ProfileGuard`), `OFFBOARDING_PORT` (8094),
`OFFBOARDING_FACTS_TOPIC` (security-events), `OFFBOARDING_PARTICIPANTS` (see above),
`OFFBOARDING_ALIVE_STALL_SEC` (240, pinned beside the liveness thresholds it is derived from),
`KAFKA_BOOTSTRAP_SERVERS` (absent = the loop never runs), `DB_URL`/`DB_USER`/`DB_PASSWORD`
(absent = in-memory H2).

**The saga's clocks are not among them, deliberately.** The purge timeout, the retry count, the
outcome-republish window, the retention and the two stall tolerances used to be environment
variables with ranges and a boot-time refusal, and no deployment in the estate ever set one. The
first two may not be dials at all: they are half of a contract with security (its safety net must
outlast `timeout x (retries + 1)`, see `SagaTimingContractTest`), and a number you cannot change
without re-deriving somebody else's is not configuration. Changing any of them means a new image,
which this service is built to survive — see the section below.

## Documentation

- [`specs/`](./specs) — the executable specifications: Gherkin, one file per use case (begin,
  record, sweep), driven through the real router by every build.
- [`Documentation.md`](./Documentation.md) — the epic → feature → story tree, generated from the
  test suite's Allure reports; regenerate with `../create-documentation.sh` after `./mvnw clean test`.

Part of a [portfolio of microservices](https://github.com/jrobertgardzinski); the deployment and
the C4 diagrams live in the workspace repo.
