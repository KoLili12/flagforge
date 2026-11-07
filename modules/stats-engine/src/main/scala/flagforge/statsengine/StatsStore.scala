package flagforge.statsengine

import cats.effect.std.Console
import cats.effect.{Async, MonadCancelThrow, Resource, Sync}
import cats.syntax.all.*
import ciris.Secret
import flagforge.domain.*
import flagforge.stats.VariationStats
import fs2.io.net.Network
import org.flywaydb.core.Flyway
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

final case class DbConfig(
    host: String,
    port: Int,
    user: String,
    password: Secret[String],
    database: String,
    poolSize: Int
):
  def jdbcUrl: String = s"jdbc:postgresql://$host:$port/$database"

/** Raw analytics events in Postgres (schema `stats`). Writes are idempotent, so Kafka redelivery is harmless.
  */
trait StatsStore[F[_]]:
  def insertExposures(exposures: List[Exposure]): F[Unit]
  def insertMetrics(events: List[MetricEvent]): F[Unit]

  /** Users exposed to each variation of `flag`. */
  def exposureCounts(env: EnvironmentKey, flag: FlagKey): F[Map[String, Long]]

  /** Metrics reported by users exposed to `flag`. */
  def metrics(env: EnvironmentKey, flag: FlagKey): F[List[String]]

  /** Sufficient statistics per variation. A metric event counts for a user only if it happened at or after
    * the user's first exposure: what happened before cannot have been caused by the variation.
    */
  def variationStats(env: EnvironmentKey, flag: FlagKey, metric: MetricKey): F[List[VariationStats]]

object StatsStore:

  private val exposure: Encoder[Exposure] = (text *: text *: text *: text *: int8 *: int8).contramap(e =>
    (e.environment.value, e.flag.value, e.contextKey, e.variation.value, e.flagVersion, e.timestamp)
  )

  private val metricEvent: Encoder[MetricEvent] =
    (text *: text *: text *: text *: float8.opt *: int8).contramap(m =>
      (m.environment.value, m.id, m.metric.value, m.contextKey, m.value, m.timestamp)
    )

  /** On conflict the earliest exposure wins, whatever order Kafka delivers them in. */
  private def insertExposures(n: Int): Command[List[Exposure]] =
    sql"""INSERT INTO stats.exposures (environment, flag, context_key, variation, flag_version, exposed_at)
          VALUES ${exposure.values.list(n)}
          ON CONFLICT (environment, flag, context_key) DO UPDATE
            SET variation = excluded.variation, flag_version = excluded.flag_version, exposed_at = excluded.exposed_at
            WHERE excluded.exposed_at < stats.exposures.exposed_at""".command

  private def insertMetrics(n: Int): Command[List[MetricEvent]] =
    sql"""INSERT INTO stats.metric_events (environment, id, metric, context_key, value, occurred_at)
          VALUES ${metricEvent.values.list(n)}
          ON CONFLICT (environment, id) DO NOTHING""".command

  private val exposureCounts: Query[(String, String), (String, Long)] =
    sql"""SELECT variation, count(*) FROM stats.exposures
          WHERE environment = $text AND flag = $text GROUP BY variation""".query(text *: int8)

  private val metricsQuery: Query[(String, String), String] =
    sql"""SELECT DISTINCT m.metric FROM stats.metric_events m
          WHERE m.environment = $text AND EXISTS (
            SELECT 1 FROM stats.exposures e
            WHERE e.environment = m.environment AND e.flag = $text AND e.context_key = m.context_key
          ) ORDER BY m.metric""".query(text)

  /** Per user: the total metric value after exposure (an event without a value counts as 1); users without
    * events contribute zeros, so `users` is everyone exposed and `conversions` everyone with at least one
    * event.
    */
  private val stats: Query[(String, String, String), VariationStats] =
    sql"""SELECT e.variation,
                 count(*)::int8,
                 count(u.total)::int8,
                 coalesce(sum(u.total), 0)::float8,
                 coalesce(sum(u.total * u.total), 0)::float8
          FROM stats.exposures e
          LEFT JOIN LATERAL (
            SELECT sum(coalesce(m.value, 1)) AS total
            FROM stats.metric_events m
            WHERE m.environment = e.environment AND m.metric = $text
              AND m.context_key = e.context_key AND m.occurred_at >= e.exposed_at
          ) u ON true
          WHERE e.environment = $text AND e.flag = $text
          GROUP BY e.variation
          ORDER BY e.variation""".query((text *: int8 *: int8 *: float8 *: float8).to[VariationStats])

  def postgres[F[_]: MonadCancelThrow](pool: Resource[F, Session[F]]): StatsStore[F] = new:

    def insertExposures(exposures: List[Exposure]): F[Unit] =
      // Postgres rejects an upsert that touches the same row twice, so collapse duplicates first.
      val unique = exposures.groupBy(_.dedupKey).values.map(_.minBy(_.timestamp)).toList
      unique.grouped(1000).toList.traverse_(chunk =>
        pool.use(_.execute(StatsStore.insertExposures(chunk.size))(chunk))
      )

    def insertMetrics(events: List[MetricEvent]): F[Unit] =
      events.distinctBy(e => (e.environment, e.id)).grouped(1000).toList.traverse_ { chunk =>
        pool.use(_.execute(StatsStore.insertMetrics(chunk.size))(chunk))
      }

    def exposureCounts(env: EnvironmentKey, flag: FlagKey): F[Map[String, Long]] =
      pool.use(_.execute(StatsStore.exposureCounts)((env.value, flag.value))).map(_.toMap)

    def metrics(env: EnvironmentKey, flag: FlagKey): F[List[String]] =
      pool.use(_.execute(metricsQuery)((env.value, flag.value)))

    def variationStats(env: EnvironmentKey, flag: FlagKey, metric: MetricKey): F[List[VariationStats]] =
      pool.use(_.execute(stats)((metric.value, env.value, flag.value)))

  def migrate[F[_]: Sync](config: DbConfig): F[Int] = Sync[F].blocking:
    Flyway
      .configure()
      .dataSource(config.jdbcUrl, config.user, config.password.value)
      .schemas("stats")
      .locations("classpath:db/stats-migration")
      .load()
      .migrate()
      .migrationsExecuted

  def pool[F[_]: Async: Network: Console](config: DbConfig): Resource[F, Resource[F, Session[F]]] =
    given Tracer[F] = Tracer.noop[F]
    given Meter[F]  = Meter.noop[F]
    builder[F](config)

  def pool[F[_]: Async: Network: Console](
      config: DbConfig,
      tracer: Tracer[F],
      meter: Meter[F]
  ): Resource[F, Resource[F, Session[F]]] =
    given Tracer[F] = tracer
    given Meter[F]  = meter
    builder[F](config)

  private def builder[F[_]: Async: Network: Console: Tracer: Meter](config: DbConfig)
      : Resource[F, Resource[F, Session[F]]] =
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password.value)
      .withDatabase(config.database)
      .pooled(config.poolSize)
