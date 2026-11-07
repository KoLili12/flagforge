package flagforge.ingest

import cats.effect.{Async, IO, IOApp, Resource}
import cats.syntax.all.*
import ciris.*
import com.comcast.ip4s.{Host, Port}
import flagforge.messaging.{AnalyticsTopics, ChangeFeed, KafkaConfig}
import flagforge.telemetry.Telemetry
import fs2.io.net.Network
import fs2.kafka.KafkaProducer
import org.http4s.HttpApp
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import scala.concurrent.duration.*

final case class IngestConfig(
    host: Host,
    port: Port,
    kafka: KafkaConfig,
    redisUri: String,
    rateLimit: RateLimit,
    dedupTtl: FiniteDuration,
    limits: IngestLimits
)

object IngestConfig:
  private given ConfigDecoder[String, Host] = ConfigDecoder[String].mapOption("Host")(Host.fromString)
  private given ConfigDecoder[Int, Port]    = ConfigDecoder[Int].mapOption("Port")(Port.fromInt)

  def load[F[_]: Async]: F[IngestConfig] =
    (
      env("HTTP_HOST").as[Host].default(Host.fromString("0.0.0.0").get),
      env("HTTP_PORT").as[Int].as[Port].default(Port.fromInt(8083).get),
      env("KAFKA_BOOTSTRAP_SERVERS").default("localhost:9092").map(KafkaConfig(_)),
      env("REDIS_URI").default("redis://localhost:6379"),
      (
        env("RATE_LIMIT_CAPACITY").as[Int].default(10_000),
        env("RATE_LIMIT_REFILL_PER_SECOND").as[Int].default(2_000)
      ).parMapN(RateLimit.apply),
      env("DEDUP_TTL").as[FiniteDuration].default(7.days),
      env("MAX_BATCH").as[Int].default(1000).map(IngestLimits(_))
    ).parMapN(IngestConfig.apply).load[F]

object IngestApp:

  def resource[F[_]: Async: LoggerFactory](
      config: IngestConfig,
      telemetry: Telemetry[F]
  ): Resource[F, HttpApp[F]] =
    require(
      config.limits.maxBatch <= config.rateLimit.capacity,
      "MAX_BATCH must not exceed RATE_LIMIT_CAPACITY"
    )
    for
      _        <- Resource.eval(AnalyticsTopics.ensure[F](config.kafka))
      guards   <- Guards.redis[F](config.redisUri, config.rateLimit, config.dedupTtl)
      producer <- KafkaProducer.resource(ChangeFeed.producerSettings[F](config.kafka).withLinger(5.millis))
      metrics  <- Resource.eval(IngestMetrics.make(telemetry.meter))
      service = IngestService(guards, EventPublisher.kafka(producer), config.limits)
      app <- Resource.eval(Telemetry.instrument(telemetry)(IngestRoutes(service, metrics).routes.orNotFound))
    yield app

  def server[F[_]: Async: Network: LoggerFactory](
      config: IngestConfig,
      telemetry: Telemetry[F]
  ): Resource[F, Server] =
    for
      app    <- resource[F](config, telemetry)
      server <-
        EmberServerBuilder.default[F].withHost(config.host).withPort(config.port).withHttpApp(app).build
      _ <- Resource.eval(LoggerFactory[F].getLogger.info(s"Event ingest on ${server.baseUri}"))
    yield server

object Main extends IOApp.Simple:
  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  def run: IO[Unit] =
    (for
      config    <- IngestConfig.load[IO].toResource
      telemetry <- Telemetry.autoConfigured[IO]("event-ingest")
      server    <- IngestApp.server[IO](config, telemetry)
    yield server).useForever
