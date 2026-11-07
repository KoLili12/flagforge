# flagforge

Feature flags & A/B testing platform written in Scala 3 on the Typelevel stack.

> Pet project, work in progress. Stages 1–2 (control plane, evaluation engine, real-time delivery, SDK) are done;
> see the roadmap.

## Architecture

```
                 ┌────────────── control plane ──────────────┐    ┌──────────── data plane ────────────┐
 curl / UI ─REST─▶ admin-api ──tx──▶ Postgres                      flag-delivery (×N) ◀──▶ Redis snapshot
                    │  eval-core      ├ flags, configs, segments          ▲                  (warm start)
                    │  (validation)   ├ audit_log                         │ consume
                    │                 └ outbox ─NOTIFY─▶ relay ─produce─▶ Kafka: flag-changes
                    │                                                     (1 partition, compacted)
                    ▼                                                         │
               Swagger UI                                   SSE: put / patch, id = offset
                                                                              ▼
                                                   your service + SDK (local evaluation, ~50 ms end-to-end)
```

A change goes: REST call → one Postgres transaction (entity + audit + outbox row) → `NOTIFY` wakes the relay →
idempotent producer → compacted Kafka topic → every `flag-delivery` instance → SSE `patch` → SDK applies it.

## Modules

| Module | Platform | Purpose |
|---|---|---|
| `domain` | JVM + JS | Flags, targeting rules, rollouts, segments, change events; refined keys (Iron); JSON codecs |
| `eval-core` | JVM + JS | Pure, deterministic evaluation, MurmurHash3 bucketing, write-time validation |
| `messaging` | JVM | The Kafka change feed: topic bootstrap, wire format, producer/consumer settings (fs2-kafka) |
| `admin-api` | JVM | Control plane: tapir + http4s REST API over Postgres (skunk, Flyway), audit log, outbox relay |
| `flag-delivery` | JVM | Data plane: consumes the feed, streams rulesets over SSE, Redis snapshots for warm starts |
| `sdk` | JVM | Client library: live ruleset over SSE, typed flags, local evaluation, reconnects |
| `it-tests` | JVM | End-to-end tests of the whole pipeline on real Postgres, Kafka and Redis (testcontainers) |

### Highlights

**Evaluation**
- **Same bucket everywhere.** Rollout bucketing hashes `(flag, salt, context key)` with the stdlib MurmurHash3;
  golden tests assert identical buckets on the JVM and in Scala.js.
- **Monotonic rollouts.** Growing a rollout from 10% to 20% never takes the feature away from anyone (property-tested).
- **Validation before persistence.** Unknown variations, bad weights, invalid regexes/semvers, unknown segments
  and prerequisite cycles are all reported at once (cats `ValidatedNec`).

**Control plane**
- **Transactional outbox.** Every change writes the entity, an audit record and a `ChangeEvent` in one transaction.
- **Low-latency relay.** A statement-level trigger `NOTIFY`s the relay on commit (polling is only a safety net);
  a transaction-scoped advisory lock keeps a single active publisher, preserving commit order.
- **Optimistic locking.** Config and segment updates carry the version the client saw; stale writes get 409.

**Data plane**
- **Offsets as versions.** The feed is a single-partition topic, so the Kafka offset is a global version all
  delivery instances agree on. It is the SSE event id: a client reconnecting to *any* instance with
  `Last-Event-ID` receives only the changes it missed (or a full `put` if it fell out of the replay window).
- **Idempotent application.** Events carry entity versions; stale and redelivered events are ignored on the server
  and in the SDK, so at-least-once delivery is safe.
- **No head-of-line blocking.** Each SSE client follows a conflated state signal; a slow client skips intermediate
  states instead of slowing down the others.
- **Warm starts.** Instances snapshot their state to Redis and seek Kafka right after the snapshot offset.

**SDK**
- `get` never touches the network and never throws: defaults plus a `Reason` on any problem.
- Flag keys are checked **at compile time**: `Flag.bool("Bad Key", false)` does not compile.
- Reconnects with exponential backoff and full jitter, resuming via `Last-Event-ID`.

## Stack

Scala 3 · cats-effect 3 · fs2 · fs2-kafka · redis4cats · http4s (Ember) · tapir (OpenAPI/Swagger) · skunk ·
Flyway · circe · Iron · ciris · log4cats · munit + ScalaCheck · testcontainers · Scala.js

## Requirements

- JDK 21+
- sbt 1.13+
- Node.js (Scala.js tests)
- Docker (local infrastructure and integration tests)

## Build & test

```bash
sbt test        # everything: unit/property tests (JVM + JS), Postgres tests, end-to-end pipeline tests
sbt itTests/test
sbt scalafmtAll
```

## Run locally

```bash
docker compose --profile full up -d --wait   # Postgres, Kafka (KRaft), Redis, Kafka UI on :8090
                                             # POSTGRES_PORT=55432 if 5432 is taken
sbt adminApi/run                             # :8080, Swagger UI at /docs; DB_PORT=55432 accordingly
REDIS_URI=redis://localhost:6379 sbt flagDelivery/run   # :8081 (use a second checkout/terminal or `java -cp`)
```

> Two `sbt` processes cannot run in the same directory at once. Either start the services from one sbt shell
> (`adminApi/bgRun`, `flagDelivery/bgRun`) or export the classpath with `sbt "export flagDelivery/Runtime/fullClasspath"`
> and run `java -cp <classpath> flagforge.delivery.Main`.

| Service | Env vars |
|---|---|
| admin-api | `HTTP_HOST`, `HTTP_PORT`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_POOL_SIZE`, `KAFKA_BOOTSTRAP_SERVERS`, `OUTBOX_RELAY_ENABLED`, `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL` |
| flag-delivery | `HTTP_HOST`, `HTTP_PORT`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_URI` (optional), `SSE_HEARTBEAT`, `SNAPSHOT_DEBOUNCE`, `MAX_RECENT_EVENTS` |

### Try it

```bash
# environment and a boolean flag (starts disabled)
curl -H 'Content-Type: application/json' -d '{"key":"production","name":"Production"}' \
  localhost:8080/api/v1/environments
curl -H 'Content-Type: application/json' -d '{"key":"new-checkout","name":"New checkout","kind":"boolean",
  "variations":[{"key":"on","value":true},{"key":"off","value":false}]}' localhost:8080/api/v1/flags

# in another terminal: watch the live stream an SDK receives
curl -N localhost:8081/sdk/v1/environments/production/stream

# turn it on for Russian users, 30% rollout for everyone else — the stream gets a `patch` immediately
curl -X PUT -H 'Content-Type: application/json' -H 'X-Actor: me' -d '{"version":1,"config":{
  "enabled":true, "offVariation":"off",
  "rules":[{"id":"ru","clauses":[{"attribute":"country","operator":"in","values":["RU"]}],
            "serve":{"type":"fixed","variation":"on"}}],
  "fallthrough":{"type":"rollout","variations":[{"variation":"on","weight":30000},{"variation":"off","weight":70000}]}
}}' localhost:8080/api/v1/environments/production/flags/new-checkout

# kill switch
curl -H 'Content-Type: application/json' -d '{"enabled":false}' \
  localhost:8080/api/v1/environments/production/flags/new-checkout/toggle

# resume a stream: only changes after offset 1 are sent
curl -N -H 'Last-Event-ID: 1' localhost:8081/sdk/v1/environments/production/stream
```

Rollout weights are in units of 1/100 000 (30 000 = 30%).

### SDK

```scala
import flagforge.sdk.*

val NewCheckout = Flag.bool("new-checkout", default = false) // key validated at compile time

FlagforgeClient
  .resource[IO](SdkConfig(uri"http://localhost:8081", EnvironmentKey.applyUnsafe("production")))
  .use { client =>
    val user = EvaluationContext("user-42", Map("country" -> Json.fromString("RU")))
    for
      _       <- client.awaitReady(5.seconds)
      enabled <- client.get(NewCheckout, user)                  // local, no network call
      _       <- client.watch(NewCheckout, user)                // every change, pushed by the server
                   .evalMap(v => IO.println(s"new-checkout = $v"))
                   .compile.drain
    yield ()
  }
```

### SSE protocol

| Event | Data | `id` |
|---|---|---|
| `put` | full `Ruleset` of the environment | feed offset the snapshot corresponds to |
| `patch` | one `ChangeEvent` (`flag_updated`, `flag_archived`, `segment_updated`) | offset of that event |
| comment `: heartbeat` | — | — |

Other endpoints: `GET /sdk/v1/environments/{env}/ruleset` (polling, `ETag`/`If-None-Match`),
`POST /sdk/v1/environments/{env}/evaluate` (server-side evaluation), `GET /ready`, `GET /health`.

## Roadmap

| Stage | Scope | Status |
|---|---|---|
| 1 | Domain, evaluation engine, admin API over Postgres | ✅ |
| 2 | Outbox relay → Kafka, `flag-delivery` with SSE + Redis warm starts, Scala SDK, end-to-end tests | ✅ |
| 3 | Exposure tracking in the SDK, `event-ingest` (Redis dedup / rate limiting), `stats-engine` (z/t-tests, SRM) | ⏳ |
| 4 | Traffic simulator, otel4s tracing, Grafana dashboards, Gatling load tests, Docker images | |
| 5 | Sequential testing (mSPRT), CUPED, Laminar UI, Kubernetes | |
