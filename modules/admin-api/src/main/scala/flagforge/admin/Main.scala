package flagforge.admin

import cats.effect.std.{Console, SecureRandom}
import cats.effect.{Async, IO, IOApp, Resource}
import flagforge.admin.db.Database
import flagforge.admin.http.Routes
import flagforge.admin.service.AdminService
import flagforge.admin.store.{EnvironmentRepository, FlagRepository, SegmentRepository}
import fs2.io.net.Network
import org.http4s.HttpApp
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.http4s.server.middleware.Logger as HttpLogger
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

object AdminApp:

  /** Everything except the HTTP server: migrations, the session pool and the wired HTTP app. */
  def httpApp[F[_]: Async: Network: Console](db: DbConfig): Resource[F, HttpApp[F]] =
    for
      _                     <- Resource.eval(Database.migrate[F](db))
      pool                  <- Database.pool[F](db)
      given SecureRandom[F] <- Resource.eval(SecureRandom.javaSecuritySecureRandom[F])
      service = AdminService.make[F](
        EnvironmentRepository.postgres(pool),
        FlagRepository.postgres(pool),
        SegmentRepository.postgres(pool)
      )
    yield Routes(service).routes.orNotFound

  def server[F[_]: Async: Network: Console: LoggerFactory](config: AppConfig): Resource[F, Server] =
    val logger = LoggerFactory[F].getLogger
    for
      app    <- httpApp[F](config.db)
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
