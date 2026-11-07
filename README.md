# flagforge

Feature flags & A/B testing platform written in Scala 3 on the Typelevel stack.

> Pet project, work in progress. Stages 1–4 (control plane, evaluation engine, real-time delivery, SDK,
> experiment analytics, containers, observability, load tests) are done; see the roadmap.

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
                                                              │ exposures + metric events (batched)
                                                              ▼
                     event-ingest ── Redis: token bucket + dedup (Lua) ──▶ Kafka: exposures, metric-events
                                                                                  │ consumer group
                                                                                  ▼
                     stats-engine ──▶ Postgres (stats) ──▶ reports: z/Welch tests, CI, lift, SRM
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
| `stats-core` | JVM + JS | Statistics from scratch: distributions, z/Welch tests, CIs, SRM, Bonferroni |
| `event-ingest` | JVM | Receives SDK analytics: Redis rate limiting and dedup (Lua), publishes to Kafka |
| `stats-engine` | JVM | Consumes analytics into Postgres, serves experiment reports |
| `sdk` | JVM | Client library: live ruleset over SSE, typed flags, local evaluation, exposure/metric tracking |
| `telemetry` | JVM | OpenTelemetry wiring (otel4s): SDK from `OTEL_*` env vars, HTTP tracing and metrics |
| `simulator` | JVM | Demo traffic: sets up an experiment and simulates users through the real SDK |
| `load-test` | JVM | SSE fan-out latency and ingest throughput against a running system |
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

**Experiments**
- **Statistics you can trust.** Special functions are implemented from scratch (JVM + Scala.js) and checked against
  scipy; simulated A/A tests confirm a ~5% false-positive rate, A/B simulations confirm power.
- **Correct attribution.** A metric event counts for a user only if it happened after their *first* exposure; the
  earliest exposure wins regardless of Kafka delivery order. One SQL aggregation yields the sufficient statistics.
- **Sample ratio mismatch** check against the configured split, Bonferroni correction for several treatments.
- **Exactly-once effect from at-least-once parts.** Ingest dedups in Redis (and releases the keys if Kafka publishing
  fails, so retries are not lost); stats-engine writes idempotently and commits offsets after writing.
- **Shared rate limits.** A token bucket in a Lua script on the Redis clock: atomic across ingest instances, no
  clock skew; `429` with `Retry-After`, which the SDK honours.
- **End-to-end proof.** A test runs a real A/B experiment through every service: 2000 simulated users, a true
  20% → 30% conversion effect, and asserts the report finds it without an SRM.

**Operations**
- **One command:** `docker compose --profile apps up` runs every service, Kafka, Redis, Postgres, Prometheus, Tempo
  and Grafana with a provisioned dashboard. Images are built by sbt-native-packager (JRE 21, non-root).
- **One trace per flag change:** the request's W3C trace context is stored with the outbox row, travels in Kafka
  headers and is continued by flag-delivery, whose span starts at the commit: admin API → SQL → Kafka → applied.
- **Load-tested** with a purpose-built tool; the numbers are below.

**SDK**
- `get` never touches the network and never throws: defaults plus a `Reason` on any problem.
- Flag keys are checked **at compile time**: `Flag.bool("Bad Key", false)` does not compile.
- Reconnects with exponential backoff and full jitter, resuming via `Last-Event-ID`.
- Exposures are recorded automatically, `track` records business events; delivery is batched, never blocks the
  application (bounded queue, drops on overflow) and flushes on close.

## Stack

Scala 3 · cats-effect 3 · fs2 · fs2-kafka · redis4cats (Lua scripting) · otel4s / OpenTelemetry · http4s (Ember) · tapir (OpenAPI/Swagger) · skunk ·
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

### Everything in Docker

```bash
sbt Docker/publishLocal                    # builds flagforge/{admin-api,flag-delivery,event-ingest,stats-engine}
docker compose --profile apps up -d --wait # infrastructure + all services
                                           # POSTGRES_PORT=55432 if 5432 is taken
```

Admin API + Swagger UI on :8080, flag delivery :8081, stats :8082, ingest :8083, Kafka UI :8090,
**Grafana :3000** (dashboard "flagforge", no login), Prometheus :9090.

Add simulated users running an A/B test, so the dashboard has something to show:

```bash
docker compose --profile apps --profile demo up -d   # USERS_PER_SECOND=25 by default
docker compose logs -f simulator                     # experiment report every 30 s
```

### Services from sbt (for development)

```bash
docker compose --profile infra up -d --wait  # Postgres, Kafka (KRaft), Redis, Kafka UI
sbt adminApi/run                             # DB_PORT=55432 if you changed POSTGRES_PORT
REDIS_URI=redis://localhost:6379 sbt flagDelivery/run
sbt eventIngest/run
sbt statsEngine/run
```

> Two `sbt` processes cannot run in the same directory at once. Either start the services from one sbt shell
> (`adminApi/bgRun`, `flagDelivery/bgRun`, ...) or run them in Docker.

| Service | Env vars |
|---|---|
| admin-api | `HTTP_HOST`, `HTTP_PORT`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_POOL_SIZE`, `KAFKA_BOOTSTRAP_SERVERS`, `OUTBOX_RELAY_ENABLED`, `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL` |
| flag-delivery | `HTTP_HOST`, `HTTP_PORT`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_URI` (optional), `SSE_HEARTBEAT`, `SNAPSHOT_DEBOUNCE`, `MAX_RECENT_EVENTS`, `MAX_CONNECTIONS` |
| all services | standard `OTEL_*` variables, e.g. `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` (telemetry is off without it) |
| event-ingest | `HTTP_HOST`, `HTTP_PORT`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_URI`, `RATE_LIMIT_CAPACITY`, `RATE_LIMIT_REFILL_PER_SECOND`, `DEDUP_TTL`, `MAX_BATCH` |
| stats-engine | `HTTP_HOST`, `HTTP_PORT`, `DB_*` (as admin-api), `KAFKA_BOOTSTRAP_SERVERS` |

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

val config = SdkConfig(
  uri"http://localhost:8081",
  EnvironmentKey.applyUnsafe("production"),
  events = EventsConfig(uri = Some(uri"http://localhost:8083")) // enables experiment tracking
)

FlagforgeClient
  .resource[IO](config)
  .use { client =>
    val user = EvaluationContext("user-42", Map("country" -> Json.fromString("RU")))
    for
      _       <- client.awaitReady(5.seconds)
      enabled <- client.get(NewCheckout, user)                  // local, no network call; records an exposure
      _       <- client.track(MetricKey("purchase"), user, Some(49.90))
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

### Experiment reports

```bash
# exposures per variation and the metrics seen for this flag
curl localhost:8082/stats/v1/environments/production/flags/new-checkout

# full report: conversion (z-test) and mean value (Welch) per variation vs control, CIs, lift, SRM
curl 'localhost:8082/stats/v1/environments/production/flags/new-checkout/metrics/purchase?control=off&weights=on:30,off:70'
```

Ingest API used by the SDK: `POST localhost:8083/events/v1/environments/{env}/batch` with
`{"exposures": [...], "metrics": [...]}` → `202 {"accepted": n, "duplicates": m}`, `400`, `429` + `Retry-After`, `503`.

## Observability

Every service exports traces and metrics over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (compose does it);
otherwise telemetry is a no-op. The collector forwards metrics to Prometheus and traces to Tempo.

The **flagforge** Grafana dashboard (regenerate with `python3 deploy/observability/tools/dashboard.py`):

| Panel | Metric |
|---|---|
| Propagation latency p50/p95/p99 | `flagforge_delivery_propagation_seconds` — outbox row written → change applied in flag-delivery |
| SSE connections, feed offset | `flagforge_delivery_sse_connections`, `flagforge_delivery_offset` |
| Change events by outcome | `flagforge_delivery_events_total{outcome=applied\|stale\|invalid}` |
| Outbox throughput and lag | `flagforge_outbox_published_total`, `flagforge_outbox_lag_seconds` (commit → Kafka ack) |
| Ingested events, rejections | `flagforge_ingest_events_total{outcome}`, `flagforge_ingest_rejected_total{reason}` |
| Stored analytics | `flagforge_stats_stored_total{kind}` |
| HTTP rate and p95 per service | `http_server_request_duration_seconds` |
| Recent flag changes | Tempo, TraceQL `{ name = "change-feed apply" }` |

## Load tests

`load-test` runs against a live system (e.g. the Docker setup):

```bash
sbt "loadTest/run sse connections=5000 rounds=10"         # SSE fan-out
sbt "loadTest/run ingest clients=50 batch=100 seconds=30" # analytics ingest
```

Results on a laptop (Apple Silicon, 8 GB for Docker Desktop, everything in containers, one instance per service,
load generator on the host — so Docker Desktop's port forwarding is part of every measurement):

| Scenario | Result |
|---|---|
| SSE fan-out, 1000 clients × 10 flag changes | 10 000/10 000 delivered; admin API call → patch at *every* client: p50 36 ms, p99 88 ms |
| SSE fan-out, 5000 clients × 10 flag changes | 100% delivered; after JIT warm-up p50 70–125 ms, p99 150–220 ms; rare single-client outliers of several seconds |
| Ingest, 50 clients × batches of 100 | **~77 000 events/s** on one instance (Redis dedup + Kafka acks), all `202`, p50 41 ms, p99 443 ms |
| stats-engine consumption | ~13 000 events/s per instance — the bottleneck; scales out to 6 instances (partitions) |
| Propagation inside the system (Grafana) | commit → applied in flag-delivery: p50 ≈ 5 ms, p99 ≈ 21 ms |

What the load tests found and fixed: ember's default limit of 1024 connections silently capped flag-delivery at
~1000 SDKs (now `MAX_CONNECTIONS`, default 20 000), and every patch was JSON-encoded once per client (now once
per change).

## Roadmap

| Stage | Scope | Status |
|---|---|---|
| 1 | Domain, evaluation engine, admin API over Postgres | ✅ |
| 2 | Outbox relay → Kafka, `flag-delivery` with SSE + Redis warm starts, Scala SDK, end-to-end tests | ✅ |
| 3 | Exposure tracking in the SDK, `event-ingest` (Redis dedup / rate limiting), `stats-engine` (z/t-tests, SRM) | ✅ |
| 4 | Docker images, otel4s tracing and metrics, Grafana dashboards, traffic simulator, load tests | ✅ |
| 5 | Sequential testing (mSPRT), CUPED, Laminar UI, Kubernetes | ⏳ |
