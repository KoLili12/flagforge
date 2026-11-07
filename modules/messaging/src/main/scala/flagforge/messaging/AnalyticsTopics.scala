package flagforge.messaging

import cats.effect.Async
import cats.syntax.all.*
import flagforge.domain.{Exposure, MetricEvent}
import fs2.kafka.ProducerRecord
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.kafka.clients.admin.NewTopic

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Topics for analytics events. Both are keyed by `environment/contextKey`, so all events of one user land in the
  * same partition of each topic (and both topics have the same partition count): a consumer that needs per-user
  * ordering or co-partitioned joins gets it for free.
  */
object AnalyticsTopics:

  val Exposures = "flagforge.exposures"
  val Metrics   = "flagforge.metric-events"

  val Partitions: Int = 6
  val Retention: FiniteDuration = 30.days

  def ensure[F[_]: Async](config: KafkaConfig): F[Unit] =
    val settings = Map("cleanup.policy" -> "delete", "retention.ms" -> Retention.toMillis.toString).asJava
    Topics.ensure(
      config.bootstrapServers,
      NewTopic(Exposures, Partitions, config.replicationFactor).configs(settings),
      NewTopic(Metrics, Partitions, config.replicationFactor).configs(settings)
    )

  def record(exposure: Exposure): ProducerRecord[String, String] =
    ProducerRecord(Exposures, s"${exposure.environment}/${exposure.contextKey}", exposure.asJson.noSpaces)

  def record(event: MetricEvent): ProducerRecord[String, String] =
    ProducerRecord(Metrics, s"${event.environment}/${event.contextKey}", event.asJson.noSpaces)

  def decodeExposure(value: String): Either[String, Exposure] = decode[Exposure](value).leftMap(_.getMessage)
  def decodeMetric(value: String): Either[String, MetricEvent] = decode[MetricEvent](value).leftMap(_.getMessage)
