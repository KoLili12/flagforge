package flagforge.admin

import cats.effect.std.{Console, SecureRandom}
import cats.effect.{Async, IO, IOApp, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import flagforge.admin.db.Database
import flagforge.admin.http.Routes
import flagforge.admin.relay.{OutboxRelay, RelayConfig}
import flagforge.admin.service.AdminService
import flagforge.admin.store.{EnvironmentRepository, FlagRepository, SegmentRepository}
import flagforge.messaging.{ChangeFeed, KafkaConfig}
import fs2.io.net.Network
import fs2.kafka.KafkaProducer
import org.http4s.HttpApp
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.http4s.server.middleware.Logger as HttpLogger
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import skunk.Session

object AdminApp:

  /** Migrated database and a session pool. */
  def database[F[_]: Async: Network: Console](db: DbConfig): Resource[F, Resource[F, Session[F]]] =
    Resource.eval(Database.migrate[F](db)) >> Database.pool[F](db)

  def routes[F[_]: Async](pool: Resource[F, Session[F]]): Resource[F, HttpApp[F]] =
    for given SecureRandom[F] <- Resource.eval(SecureRandom.javaSecuritySecureRandom[F])
    yield
      val service = AdminService.make[F](
        EnvironmentRepository.postgres(pool),
        FlagRepository.postgres(pool),
        SegmentRepository.postgres(pool)
      )
      Routes(service).routes.orNotFound

  /** Everything except the HTTP server and the relay: migrations, the session pool and the wired HTTP app. */
  def httpApp[F[_]: Async: Network: Console](db: DbConfig): Resource[F, HttpApp[F]] =
    database[F](db).flatMap(routes[F])

  /** Background outbox → Kafka relay. Ensures the change feed topic exists first. */
  def relay[F[_]: Async: LoggerFactory](
      pool: Resource[F, Session[F]],
      kafka: KafkaConfig,
      config: RelayConfig
  ): Resource[F, Unit] =
    for
      _        <- Resource.eval(ChangeFeed.ensureTopic[F](kafka))
      producer <- KafkaProducer.resource(ChangeFeed.producerSettings[F](kafka))
      _        <- OutboxRelay(pool, producer, config).run.background
    yield ()

  def server[F[_]: Async: Network: Console: LoggerFactory](config: AppConfig): Resource[F, Server] =
    val logger = LoggerFactory[F].getLogger
    for
      pool   <- database[F](config.db)
      app    <- routes[F](pool)
      _ <-
        if config.relay.enabled then relay[F](pool, config.kafka, config.relay)
        else Resource.eval(logger.warn("Outbox relay is disabled: changes will not reach the data plane"))
      server <- EmberServerBuilder
        .default[F]
        .withHost(config.http.host)
        .withPort(config.http.port)
        .withHttpApp(HttpLogger.httpApp(logHeaders = false, logBody = false)(app))
        .build
      _ <- Resource.eval(logger.info(s"Admin API on ${server.baseUri}, Swagger UI: ${server.baseUri}docs"))
    yield server

object Main extends IOApp.Simple:
  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  def run: IO[Unit] = AppConfig.load[IO].toResource.flatMap(AdminApp.server[IO]).useForever
