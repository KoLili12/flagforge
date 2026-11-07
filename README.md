# flagforge

Feature flags & A/B testing platform written in Scala 3 on the Typelevel stack.

> Pet project. Work in progress — see the roadmap below.

## Modules

| Module | Platform | Purpose |
|---|---|---|
| `domain` | JVM + JS | Domain model (flags, rules, segments) and JSON codecs |
| `eval-core` | JVM + JS | Pure, deterministic flag evaluation and bucketing |
| `admin-api` | JVM | Control plane: REST API over Postgres |

## Requirements

- JDK 21+
- sbt 1.13+
- Docker (for local infrastructure and integration tests)

## Build

```bash
sbt compile
sbt test
```
