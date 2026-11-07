package flagforge.it

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import ciris.Secret
import com.comcast.ip4s.*
import com.dimafeng.testcontainers.{GenericContainer, PostgreSQLContainer}
import flagforge.admin.relay.RelayConfig
import flagforge.admin.{AdminApp, DbConfig}
import flagforge.delivery.{DeliveryApp, DeliveryConfig, RedisSnapshotStore, SnapshotStore}
import flagforge.ingest.{IngestApp, IngestConfig, IngestLimits, RateLimit}
import flagforge.statsengine.{StatsApp, StatsConfig}
import flagforge.messaging.KafkaConfig
import io.circe.Json
import io.circe.parser.parse
import org.http4s.*
import org.http4s.headers.`Content-Type`
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import org.typelevel.ci.*
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import skunk.Session

import scala.concurrent.duration.*

/** The whole system wired together against real infrastructure:
  *
  *   - flags: admin API (in-process) → Postgres outbox → relay → Kafka → flag-delivery (HTTP) → SSE → SDK
  *   - analytics: SDK → event-ingest (HTTP) → Redis dedup/limits → Kafka → stats-engine (in-process) → Postgres
  */
final case class Pipeline(
    admin: HttpApp[IO],
    adminDb: Resource[IO, Session[IO]],
    deliveryUri: Uri,
    deliveryConfig: DeliveryConfig,
    ingestUri: Uri,
    stats: HttpApp[IO],
    redisUri: String
):
  def call(method: Method, path: String, body: Option[Json] = None): IO[(Status, Json)] =
    val base    = Request[IO](method, Uri.unsafeFromString(path)).putHeaders(Header.Raw(ci"X-Actor", "e2e"))
    val request =
      body.fold(base)(json =>
        base.withEntity(json.noSpaces).withContentType(`Content-Type`(MediaType.application.json))
      )
    admin.run(request).flatMap(r => r.as[String].map(t => (r.status, parse(t).getOrElse(Json.fromString(t)))))

  def snapshots: Resource[IO, SnapshotStore[IO]] =
    RedisSnapshotStore.resource[IO](redisUri)(using summon, Pipeline.loggerFactory)

object Pipeline:
  given loggerFactory: LoggerFactory[IO] = Slf4jFactory.create[IO]

  private def container[C](start: => C)(stop: C => Unit): Resource[IO, C] =
    Resource.make(IO.blocking(start))(c => IO.blocking(stop(c)))

  private val postgres = container {
    val c = PostgreSQLContainer.Def(DockerImageName.parse("postgres:17-alpine")).createContainer()
    c.start(); c
  }(_.stop())

  private val kafka = container {
    val c = KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"))
    c.start(); c
  }(_.stop())

  private val redis = container {
    val c =
      GenericContainer("redis:7.4-alpine", exposedPorts = Seq(6379), waitStrategy = Wait.forListeningPort())
    c.start(); c
  }(_.stop())

  val resource: Resource[IO, Pipeline] =
    for
      (pg, kafkaContainer, redisContainer) <- (postgres, kafka, redis).parTupled
      db = DbConfig(
        pg.host,
        pg.mappedPort(5432),
        pg.username,
        Secret(pg.password),
        pg.databaseName,
        poolSize = 6
      )
      kafkaConfig = KafkaConfig(kafkaContainer.getBootstrapServers)
      redisUri    = s"redis://${redisContainer.host}:${redisContainer.mappedPort(6379)}"
      pool  <- AdminApp.database[IO](db)
      admin <- AdminApp.routes[IO](pool)
      _     <- AdminApp.relay[IO](
        pool,
        kafkaConfig,
        RelayConfig(enabled = true, batchSize = 100, pollInterval = 1.second)
      )
      deliveryConfig = DeliveryConfig(
        host"127.0.0.1",
        port"0",
        kafkaConfig,
        Some(redisUri),
        heartbeat = 2.seconds,
        snapshotDebounce = 200.millis
      )
      snapshots <- RedisSnapshotStore.resource[IO](redisUri)
      delivery  <- DeliveryApp.server[IO](deliveryConfig, snapshots)
      ingest <- IngestApp.server[IO](
        IngestConfig(
          host"127.0.0.1",
          port"0",
          kafkaConfig,
          redisUri,
          RateLimit(capacity = 100_000, refillPerSecond = 50_000),
          dedupTtl = 1.hour,
          IngestLimits()
        )
      )
      stats <- StatsApp.resource[IO](
        StatsConfig(
          host"127.0.0.1",
          port"0",
          flagforge.statsengine.DbConfig(db.host, db.port, db.user, db.password, db.database, poolSize = 4),
          kafkaConfig
        )
      )
    yield Pipeline(admin, pool, delivery.baseUri, deliveryConfig, ingest.baseUri, stats, redisUri)
