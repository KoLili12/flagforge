# flagforge

Feature flags & A/B testing platform written in Scala 3 on the Typelevel stack.

> Pet project, work in progress. Stage 1 (control plane + evaluation engine) is done, see the roadmap.

## Architecture

```
 Admin UI / curl ──REST──▶ admin-api ──▶ Postgres ──outbox──▶ Kafka: flag-changes (compacted)   [stage 2]
                              │                                        │
                              └── eval-core (validation, preview)      ▼
                                                              flag-delivery ──SSE──▶ SDK (local evaluation)
                                                                                      │ exposures
                                                                                      ▼
                                                                         event-ingest → Kafka → stats-engine
```

## Modules

| Module | Platform | Purpose |
|---|---|---|
| `domain` | JVM + JS | Flags, targeting rules, rollouts, segments, events; refined keys (Iron); JSON codecs |
| `eval-core` | JVM + JS | Pure, deterministic evaluation, MurmurHash3 bucketing, write-time validation |
| `admin-api` | JVM | Control plane: tapir + http4s REST API over Postgres (skunk, Flyway), audit log, transactional outbox |

### Highlights

- **Same bucket everywhere.** Rollout bucketing hashes `(flag, salt, context key)` with the stdlib MurmurHash3;
  golden tests assert identical buckets on the JVM and in Scala.js.
- **Monotonic rollouts.** Growing a rollout from 10% to 20% never takes the feature away from anyone (property-tested).
- **Validation before persistence.** Unknown variations, bad weights, invalid regexes/semvers, unknown segments
  and prerequisite cycles are all reported at once (cats `ValidatedNec`).
- **Transactional outbox.** Every change writes the entity, an audit record and a `ChangeEvent` in a single
  transaction, ready to be relayed to a compacted Kafka topic.
- **Optimistic locking.** Config and segment updates carry the version the client saw; stale writes get 409.

## Stack

Scala 3 · cats-effect 3 · fs2 · http4s (Ember) · tapir (OpenAPI/Swagger) · skunk · Flyway · circe · Iron ·
ciris · log4cats · munit + ScalaCheck · testcontainers · Scala.js

## Requirements

- JDK 21+
- sbt 1.13+
- Node.js (Scala.js tests)
- Docker (local infrastructure and integration tests)

## Build & test

```bash
sbt test        # unit + property tests (JVM and JS) and Postgres integration tests via testcontainers
sbt scalafmtAll
```

## Run locally

```bash
docker compose up -d --wait postgres     # POSTGRES_PORT=55432 if 5432 is taken
sbt adminApi/run                         # DB_PORT=55432 accordingly; HTTP on :8080
```

Swagger UI: http://localhost:8080/docs

Configuration (env vars): `HTTP_HOST`, `HTTP_PORT`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`,
`DB_POOL_SIZE`.

Full infrastructure for the next stages: `docker compose --profile full up -d` (adds Redis, Kafka in KRaft mode and
Kafka UI on :8090).

### Try it

```bash
# environment and a boolean flag (starts disabled)
curl -H 'Content-Type: application/json' -d '{"key":"production","name":"Production"}' \
  localhost:8080/api/v1/environments
curl -H 'Content-Type: application/json' -d '{"key":"new-checkout","name":"New checkout","kind":"boolean",
  "variations":[{"key":"on","value":true},{"key":"off","value":false}]}' localhost:8080/api/v1/flags

# turn it on for Russian users, 30% rollout for everyone else
curl -X PUT -H 'Content-Type: application/json' -H 'X-Actor: me' -d '{"version":1,"config":{
  "enabled":true, "offVariation":"off",
  "rules":[{"id":"ru","clauses":[{"attribute":"country","operator":"in","values":["RU"]}],
            "serve":{"type":"fixed","variation":"on"}}],
  "fallthrough":{"type":"rollout","variations":[{"variation":"on","weight":30000},{"variation":"off","weight":70000}]}
}}' localhost:8080/api/v1/environments/production/flags/new-checkout

# evaluate for a context
curl -H 'Content-Type: application/json' -d '{"key":"user-42","attributes":{"country":"RU"}}' \
  localhost:8080/api/v1/environments/production/evaluate

# kill switch
curl -H 'Content-Type: application/json' -d '{"enabled":false}' \
  localhost:8080/api/v1/environments/production/flags/new-checkout/toggle
```

Rollout weights are in units of 1/100 000 (30 000 = 30%).

## Roadmap

| Stage | Scope | Status |
|---|---|---|
| 1 | Domain, evaluation engine, admin API over Postgres | ✅ |
| 2 | Outbox relay → Kafka (fs2-kafka), `flag-delivery` with SSE + Redis, Scala SDK | ⏳ |
| 3 | `event-ingest` (exposures, Redis dedup / rate limiting), `stats-engine` (z/t-tests, SRM) | |
| 4 | Traffic simulator, otel4s tracing, Grafana dashboards, Gatling load tests | |
| 5 | Sequential testing (mSPRT), CUPED, Laminar UI, Kubernetes | |
