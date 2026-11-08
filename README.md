# flagforge

Платформа фича-флагов и A/B-тестов на Scala 3 и стеке Typelevel.

Меняешь флаг через REST API — и все подключённые сервисы видят изменение за десятки миллисекунд по SSE.
SDK вычисляет флаги локально, отправляет exposure-события и бизнес-метрики, а платформа отвечает, сработал ли
эксперимент: z-тест и t-тест Уэлча, доверительные интервалы, lift и проверка на sample ratio mismatch.

- **Доставка в реальном времени:** transactional outbox → Kafka → SSE, переподключение к любому инстансу
  с продолжением через `Last-Event-ID`; распространение изменения внутри системы p50 ≈ 5 мс, 1000 SDK-клиентов
  получают обновление с p99 88 мс.
- **Пайплайн аналитики:** rate limiting и дедупликация в Redis (Lua) → Kafka → Postgres, ~77 000 событий/с
  на приёме одним инстансом; эффект exactly-once, собранный из at-least-once компонентов.
- **Статистика с нуля**, кросс-компилируется в Scala.js и сверена со scipy.
- **Эксплуатация как в проде:** Docker-образы, вся система через `docker compose`, трейсы OpenTelemetry,
  которые проходят за изменением флага через все сервисы, готовый дашборд Grafana, нагрузочные тесты.
- **181 тест:** unit- и property-тесты на JVM и JS, end-to-end тесты на настоящих Postgres, Kafka и Redis.

```bash
sbt Docker/publishLocal && docker compose --profile apps --profile demo up -d --wait
# Swagger UI: localhost:8080/docs · Grafana: localhost:3000 · симулированный A/B-тест: docker compose logs -f simulator
```

## Архитектура

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

Путь изменения: REST-запрос → одна транзакция в Postgres (сущность + аудит + строка outbox) → `NOTIFY` будит
relay → идемпотентный producer → compacted-топик Kafka → каждый инстанс `flag-delivery` → SSE `patch` → SDK
применяет изменение.

## Модули

| Модуль | Платформа | Назначение |
|---|---|---|
| `domain` | JVM + JS | Флаги, правила таргетинга, rollout'ы, сегменты, события изменений; refined-ключи (Iron); JSON-кодеки |
| `eval-core` | JVM + JS | Чистое детерминированное вычисление флагов, бакетинг на MurmurHash3, валидация при записи |
| `messaging` | JVM | Лента изменений в Kafka: создание топиков, формат сообщений, настройки producer/consumer (fs2-kafka) |
| `admin-api` | JVM | Control plane: REST API на tapir + http4s поверх Postgres (skunk, Flyway), журнал аудита, outbox relay |
| `flag-delivery` | JVM | Data plane: читает ленту, стримит наборы правил по SSE, снапшоты в Redis для быстрого старта |
| `stats-core` | JVM + JS | Статистика с нуля: распределения, z- и t-тесты Уэлча, доверительные интервалы, SRM, поправка Бонферрони |
| `event-ingest` | JVM | Приём аналитики от SDK: rate limiting и дедупликация в Redis (Lua), публикация в Kafka |
| `stats-engine` | JVM | Складывает аналитику в Postgres, отдаёт отчёты по экспериментам |
| `sdk` | JVM | Клиентская библиотека: живой набор правил по SSE, типизированные флаги, локальное вычисление, трекинг exposure и метрик |
| `telemetry` | JVM | Подключение OpenTelemetry (otel4s): SDK из переменных `OTEL_*`, трейсинг и метрики HTTP |
| `simulator` | JVM | Демо-трафик: настраивает эксперимент и симулирует пользователей через настоящий SDK |
| `load-test` | JVM | Задержка SSE fan-out и пропускная способность приёма событий на работающей системе |
| `it-tests` | JVM | End-to-end тесты всего пайплайна на настоящих Postgres, Kafka и Redis (testcontainers) |

### Ключевые решения

**Вычисление флагов**
- **Одинаковый бакет везде.** Бакетинг rollout'а хэширует `(flag, salt, context key)` стандартным MurmurHash3;
  golden-тесты проверяют, что бакеты совпадают на JVM и в Scala.js.
- **Монотонные rollout'ы.** Увеличение rollout'а с 10% до 20% ни у кого не отнимает фичу (проверено property-тестами).
- **Валидация до записи.** Неизвестные вариации, неверные веса, некорректные regex и semver, неизвестные сегменты
  и циклы в prerequisite-флагах сообщаются все сразу (cats `ValidatedNec`).

**Control plane**
- **Transactional outbox.** Каждое изменение записывает сущность, запись аудита и `ChangeEvent` в одной транзакции.
- **Relay с низкой задержкой.** Statement-level триггер делает `NOTIFY` при коммите (polling — только страховка);
  advisory lock на уровне транзакции оставляет одного активного публикатора и сохраняет порядок коммитов.
- **Оптимистичные блокировки.** Обновления конфигурации и сегментов передают версию, которую видел клиент;
  устаревшая запись получает 409.

**Data plane**
- **Offset как версия.** Лента — топик из одной партиции, поэтому offset в Kafka — глобальная версия, о которой
  договорились все инстансы доставки. Он же — id SSE-события: клиент, переподключившийся к *любому* инстансу
  с `Last-Event-ID`, получает только пропущенные изменения (или полный `put`, если выпал из окна повтора).
- **Идемпотентное применение.** События несут версии сущностей; устаревшие и повторно доставленные события
  игнорируются и на сервере, и в SDK, поэтому доставка at-least-once безопасна.
- **Нет head-of-line blocking.** Каждый SSE-клиент следит за схлопывающимся сигналом состояния; медленный клиент
  пропускает промежуточные состояния, а не тормозит остальных.
- **Быстрый старт.** Инстансы сохраняют состояние в Redis и начинают читать Kafka сразу после offset'а снапшота.

**Эксперименты**
- **Статистике можно доверять.** Специальные функции реализованы с нуля (JVM + Scala.js) и сверены со scipy;
  симулированные A/A-тесты подтверждают ~5% ложных срабатываний, A/B-симуляции подтверждают мощность.
- **Корректная атрибуция.** Событие метрики засчитывается пользователю, только если оно произошло после его
  *первого* exposure; побеждает самый ранний exposure независимо от порядка доставки в Kafka. Одна SQL-агрегация
  даёт достаточные статистики.
- **Проверка sample ratio mismatch** относительно заданного сплита, поправка Бонферрони для нескольких вариантов.
- **Эффект exactly-once из at-least-once компонентов.** Приём дедуплицирует в Redis (и освобождает ключи, если
  публикация в Kafka не удалась, чтобы ретраи не терялись); stats-engine пишет идемпотентно и коммитит offset'ы
  после записи.
- **Общие лимиты.** Token bucket в Lua-скрипте на часах Redis: атомарен для всех инстансов приёма, без рассинхрона
  часов; `429` с `Retry-After`, который SDK соблюдает.
- **Сквозное доказательство.** Тест прогоняет настоящий A/B-эксперимент через все сервисы: 2000 симулированных
  пользователей, реальный эффект конверсии 20% → 30%, и проверяет, что отчёт его находит без SRM.

**Эксплуатация**
- **Одна команда:** `docker compose --profile apps up` поднимает все сервисы, Kafka, Redis, Postgres, Prometheus,
  Tempo и Grafana с готовым дашбордом. Образы собирает sbt-native-packager (JRE 21, не root).
- **Один трейс на изменение флага:** W3C trace context запроса сохраняется вместе со строкой outbox, едет в
  заголовках Kafka и продолжается в flag-delivery, чей span начинается в момент коммита:
  admin API → SQL → Kafka → применено.
- **Нагрузочное тестирование** собственным инструментом; цифры ниже.

**SDK**
- `get` никогда не ходит в сеть и не бросает исключений: при любой проблеме — значение по умолчанию и `Reason`.
- Ключи флагов проверяются **на этапе компиляции**: `Flag.bool("Bad Key", false)` не скомпилируется.
- Переподключение с экспоненциальной задержкой и full jitter, продолжение через `Last-Event-ID`.
- Exposure записываются автоматически, `track` записывает бизнес-события; отправка батчами, никогда не блокирует
  приложение (ограниченная очередь, при переполнении события отбрасываются) и дочищается при закрытии.

## Стек

Scala 3 · cats-effect 3 · fs2 · fs2-kafka · redis4cats (Lua-скрипты) · otel4s / OpenTelemetry · http4s (Ember) · tapir (OpenAPI/Swagger) · skunk ·
Flyway · circe · Iron · ciris · log4cats · munit + ScalaCheck · testcontainers · Scala.js

## Требования

- JDK 21+
- sbt 1.13+
- Node.js (тесты Scala.js)
- Docker (локальная инфраструктура и интеграционные тесты)

## Сборка и тесты

```bash
sbt test        # всё: unit/property-тесты (JVM + JS), тесты на Postgres, end-to-end тесты пайплайна
sbt itTests/test
sbt scalafmtAll
```

## Локальный запуск

### Всё в Docker

```bash
sbt Docker/publishLocal                    # собирает flagforge/{admin-api,flag-delivery,event-ingest,stats-engine}
docker compose --profile apps up -d --wait # инфраструктура + все сервисы
                                           # POSTGRES_PORT=55432, если 5432 занят
```

Admin API и Swagger UI на :8080, доставка флагов :8081, статистика :8082, приём событий :8083, Kafka UI :8090,
**Grafana :3000** (дашборд «flagforge», без логина), Prometheus :9090.

Добавить симулированных пользователей с A/B-тестом, чтобы на дашборде было что показать:

```bash
docker compose --profile apps --profile demo up -d   # по умолчанию USERS_PER_SECOND=25
docker compose logs -f simulator                     # отчёт по эксперименту каждые 30 с
```

Остановить: `docker compose --profile apps --profile demo down` (добавь `-v`, чтобы удалить и данные).

### Сервисы из sbt (для разработки)

```bash
docker compose --profile infra up -d --wait  # Postgres, Kafka (KRaft), Redis, Kafka UI
sbt adminApi/run                             # DB_PORT=55432, если меняли POSTGRES_PORT
REDIS_URI=redis://localhost:6379 sbt flagDelivery/run
sbt eventIngest/run
sbt statsEngine/run
```

> Два процесса `sbt` не могут одновременно работать в одной директории. Либо запускай сервисы из одного
> sbt shell (`adminApi/bgRun`, `flagDelivery/bgRun`, ...), либо в Docker.

| Сервис | Переменные окружения |
|---|---|
| admin-api | `HTTP_HOST`, `HTTP_PORT`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `DB_POOL_SIZE`, `KAFKA_BOOTSTRAP_SERVERS`, `OUTBOX_RELAY_ENABLED`, `OUTBOX_BATCH_SIZE`, `OUTBOX_POLL_INTERVAL` |
| flag-delivery | `HTTP_HOST`, `HTTP_PORT`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_URI` (необязательно), `SSE_HEARTBEAT`, `SNAPSHOT_DEBOUNCE`, `MAX_RECENT_EVENTS`, `MAX_CONNECTIONS` |
| event-ingest | `HTTP_HOST`, `HTTP_PORT`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_URI`, `RATE_LIMIT_CAPACITY`, `RATE_LIMIT_REFILL_PER_SECOND`, `DEDUP_TTL`, `MAX_BATCH` |
| stats-engine | `HTTP_HOST`, `HTTP_PORT`, `DB_*` (как у admin-api), `KAFKA_BOOTSTRAP_SERVERS` |
| все сервисы | стандартные переменные `OTEL_*`, например `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317` (без неё телеметрия выключена) |

### Попробовать руками

```bash
# окружение и булев флаг (создаётся выключенным)
curl -H 'Content-Type: application/json' -d '{"key":"production","name":"Production"}' \
  localhost:8080/api/v1/environments
curl -H 'Content-Type: application/json' -d '{"key":"new-checkout","name":"New checkout","kind":"boolean",
  "variations":[{"key":"on","value":true},{"key":"off","value":false}]}' localhost:8080/api/v1/flags

# в другом терминале: живой поток, который получает SDK
curl -N localhost:8081/sdk/v1/environments/production/stream

# включить для пользователей из России, 30% rollout для остальных — в поток сразу приходит `patch`
curl -X PUT -H 'Content-Type: application/json' -H 'X-Actor: me' -d '{"version":1,"config":{
  "enabled":true, "offVariation":"off",
  "rules":[{"id":"ru","clauses":[{"attribute":"country","operator":"in","values":["RU"]}],
            "serve":{"type":"fixed","variation":"on"}}],
  "fallthrough":{"type":"rollout","variations":[{"variation":"on","weight":30000},{"variation":"off","weight":70000}]}
}}' localhost:8080/api/v1/environments/production/flags/new-checkout

# kill switch
curl -H 'Content-Type: application/json' -d '{"enabled":false}' \
  localhost:8080/api/v1/environments/production/flags/new-checkout/toggle

# продолжить поток: придут только изменения после offset 1
curl -N -H 'Last-Event-ID: 1' localhost:8081/sdk/v1/environments/production/stream
```

Веса rollout'а задаются в единицах 1/100 000 (30 000 = 30%).

### SDK

```scala
import flagforge.sdk.*

val NewCheckout = Flag.bool("new-checkout", default = false) // ключ проверяется при компиляции

val config = SdkConfig(
  uri"http://localhost:8081",
  EnvironmentKey.applyUnsafe("production"),
  events = EventsConfig(uri = Some(uri"http://localhost:8083")) // включает трекинг экспериментов
)

FlagforgeClient
  .resource[IO](config)
  .use { client =>
    val user = EvaluationContext("user-42", Map("country" -> Json.fromString("RU")))
    for
      _       <- client.awaitReady(5.seconds)
      enabled <- client.get(NewCheckout, user)                  // локально, без сети; записывает exposure
      _       <- client.track(MetricKey("purchase"), user, Some(49.90))
      _       <- client.watch(NewCheckout, user)                // каждое изменение, присланное сервером
                   .evalMap(v => IO.println(s"new-checkout = $v"))
                   .compile.drain
    yield ()
  }
```

### Протокол SSE

| Событие | Данные | `id` |
|---|---|---|
| `put` | полный `Ruleset` окружения | offset ленты, которому соответствует снапшот |
| `patch` | один `ChangeEvent` (`flag_updated`, `flag_archived`, `segment_updated`) | offset этого события |
| комментарий `: heartbeat` | — | — |

Другие эндпоинты: `GET /sdk/v1/environments/{env}/ruleset` (polling, `ETag`/`If-None-Match`),
`POST /sdk/v1/environments/{env}/evaluate` (вычисление на сервере), `GET /ready`, `GET /health`.

### Отчёты по экспериментам

```bash
# exposure по вариациям и метрики, встреченные для этого флага
curl localhost:8082/stats/v1/environments/production/flags/new-checkout

# полный отчёт: конверсия (z-тест) и среднее значение (Уэлч) по вариациям против контроля, CI, lift, SRM
curl 'localhost:8082/stats/v1/environments/production/flags/new-checkout/metrics/purchase?control=off&weights=on:30,off:70'
```

API приёма, которым пользуется SDK: `POST localhost:8083/events/v1/environments/{env}/batch` с телом
`{"exposures": [...], "metrics": [...]}` → `202 {"accepted": n, "duplicates": m}`, `400`, `429` + `Retry-After`, `503`.

## Наблюдаемость

Каждый сервис экспортирует трейсы и метрики по OTLP, если задана `OTEL_EXPORTER_OTLP_ENDPOINT` (compose её задаёт);
иначе телеметрия — no-op. Коллектор отправляет метрики в Prometheus, трейсы — в Tempo.

Дашборд Grafana **flagforge** (пересоздать: `python3 deploy/observability/tools/dashboard.py`):

| Панель | Метрика |
|---|---|
| Задержка распространения p50/p95/p99 | `flagforge_delivery_propagation_seconds` — от записи строки outbox до применения в flag-delivery |
| SSE-соединения, offset ленты | `flagforge_delivery_sse_connections`, `flagforge_delivery_offset` |
| События изменений по исходу | `flagforge_delivery_events_total{outcome=applied\|stale\|invalid}` |
| Пропускная способность и лаг outbox | `flagforge_outbox_published_total`, `flagforge_outbox_lag_seconds` (коммит → ack от Kafka) |
| Принятые события, отказы | `flagforge_ingest_events_total{outcome}`, `flagforge_ingest_rejected_total{reason}` |
| Сохранённая аналитика | `flagforge_stats_stored_total{kind}` |
| HTTP rate и p95 по сервисам | `http_server_request_duration_seconds` |
| Последние изменения флагов | Tempo, TraceQL `{ name = "change-feed apply" }` |

## Нагрузочные тесты

`load-test` запускается против работающей системы (например, Docker-сборки):

```bash
sbt "loadTest/run sse connections=5000 rounds=10"         # SSE fan-out
sbt "loadTest/run ingest clients=50 batch=100 seconds=30" # приём аналитики
```

Результаты на ноутбуке (Apple Silicon, 8 ГБ для Docker Desktop, всё в контейнерах, по одному инстансу каждого
сервиса, генератор нагрузки на хосте — поэтому проброс портов Docker Desktop входит в каждое измерение):

| Сценарий | Результат |
|---|---|
| SSE fan-out, 1000 клиентов × 10 изменений флага | доставлено 10 000/10 000; от вызова admin API до `patch` у *каждого* клиента: p50 36 мс, p99 88 мс |
| SSE fan-out, 5000 клиентов × 10 изменений флага | доставлено 100%; после прогрева JIT p50 70–125 мс, p99 150–220 мс; редкие выбросы у отдельных клиентов до нескольких секунд |
| Приём, 50 клиентов × батчи по 100 | **~77 000 событий/с** на одном инстансе (дедупликация в Redis + ack от Kafka), все `202`, p50 41 мс, p99 443 мс |
| Обработка в stats-engine | ~13 000 событий/с на инстанс — узкое место; масштабируется до 6 инстансов (по числу партиций) |
| Распространение внутри системы (Grafana) | от коммита до применения в flag-delivery: p50 ≈ 5 мс, p99 ≈ 21 мс |

Что нашли и исправили нагрузочные тесты: стандартный лимит Ember в 1024 соединения молча ограничивал
flag-delivery ~1000 SDK-клиентами (теперь `MAX_CONNECTIONS`, по умолчанию 20 000), а каждый `patch`
сериализовался в JSON отдельно для каждого клиента (теперь один раз на изменение).
