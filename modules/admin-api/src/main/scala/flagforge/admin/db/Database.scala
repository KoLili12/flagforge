package flagforge.admin.db

import cats.effect.std.Console
import cats.effect.{Async, Resource, Sync}
import flagforge.admin.DbConfig
import flagforge.telemetry.Telemetry
import fs2.io.net.Network
import org.flywaydb.core.Flyway
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import skunk.Session

object Database:

  def pool[F[_]: Async: Network: Console](config: DbConfig): Resource[F, Resource[F, Session[F]]] =
    pool(config, Telemetry.noop[F])

  /** Pool of skunk sessions; every query becomes a span of the current trace. */
  def pool[F[_]: Async: Network: Console](
      config: DbConfig,
      telemetry: Telemetry[F]
  ): Resource[F, Resource[F, Session[F]]] =
    given Tracer[F] = telemetry.tracer
    given Meter[F]  = telemetry.meter
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password.value)
      .withDatabase(config.database)
      .pooled(config.poolSize)

  /** Applies pending Flyway migrations from `classpath:db/migration`. Returns how many were applied. */
  def migrate[F[_]: Sync](config: DbConfig): F[Int] = Sync[F].blocking:
    Flyway
      .configure()
      .dataSource(config.jdbcUrl, config.user, config.password.value)
      .locations("classpath:db/migration")
      .load()
      .migrate()
      .migrationsExecuted
