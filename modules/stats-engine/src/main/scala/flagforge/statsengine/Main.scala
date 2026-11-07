package flagforge.statsengine

import cats.effect.std.Console
import cats.effect.syntax.all.*
import cats.effect.{Async, IO, IOApp, Resource}
import cats.syntax.all.*
import ciris.*
import com.comcast.ip4s.{Host, Port}
import flagforge.messaging.{AnalyticsTopics, KafkaConfig}
import fs2.io.net.Network
import org.http4s.HttpApp
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import scala.concurrent.duration.*

final case class StatsConfig(host: Host, port: Port, db: DbConfig, kafka: KafkaConfig)

object StatsConfig:
  private given ConfigDecoder[String, Host] = ConfigDecoder[String].mapOption("Host")(Host.fromString)
  private given ConfigDecoder[Int, Port]    = ConfigDecoder[Int].mapOption("Port")(Port.fromInt)

  def load[F[_]: Async]: F[StatsConfig] =
    (
      env("HTTP_HOST").as[Host].default(Host.fromString("0.0.0.0").get),
      env("HTTP_PORT").as[Int].as[Port].default(Port.fromInt(8082).get),
      (
        env("DB_HOST").default("localhost"),
        env("DB_PORT").as[Int].default(5432),
        env("DB_USER").default("flagforge"),
        env("DB_PASSWORD").default("flagforge").secret,
        env("DB_NAME").default("flagforge"),
        env("DB_POOL_SIZE").as[Int].default(10)
      ).parMapN(DbConfig.apply),
      env("KAFKA_BOOTSTRAP_SERVERS").default("localhost:9092").map(KafkaConfig(_))
    ).parMapN(StatsConfig.apply).load[F]

object StatsApp:

  def resource[F[_]: Async: Network: Console: LoggerFactory](config: StatsConfig): Resource[F, HttpApp[F]] =
    val logger                           = LoggerFactory[F].getLogger
    def supervised(fa: F[Unit]): F[Unit] = (fa >> logger.warn("Analytics consumer stopped, restarting"))
      .handleErrorWith(e =>
        logger.error(e)("Analytics consumer failed, restarting in 5s") >> Async[F].sleep(5.seconds)
      )
      .foreverM
    for
      _    <- Resource.eval(StatsStore.migrate[F](config.db))
      _    <- Resource.eval(AnalyticsTopics.ensure[F](config.kafka))
      pool <- StatsStore.pool[F](config.db)
      store = StatsStore.postgres(pool)
      _ <- supervised(EventConsumer(config.kafka, store).run).background
    yield ReportRoutes(store).routes.orNotFound

  def server[F[_]: Async: Network: Console: LoggerFactory](config: StatsConfig): Resource[F, Server] =
    for
      app    <- resource[F](config)
      server <-
        EmberServerBuilder.default[F].withHost(config.host).withPort(config.port).withHttpApp(app).build
      _ <- Resource.eval(LoggerFactory[F].getLogger.info(s"Stats engine on ${server.baseUri}"))
    yield server

object Main extends IOApp.Simple:
  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  def run: IO[Unit] = StatsConfig.load[IO].toResource.flatMap(StatsApp.server[IO]).useForever
