package flagforge.messaging

import cats.effect.Async
import cats.syntax.all.*
import flagforge.domain.ChangeEvent
import fs2.kafka.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException

import scala.jdk.CollectionConverters.*

final case class KafkaConfig(
    bootstrapServers: String,
    topic: String = ChangeEvent.Topic,
    replicationFactor: Short = 1
)

/** The change feed: a single-partition, log-compacted topic of [[ChangeEvent]]s.
  *
  *   - One partition gives a total order of all changes, so a record offset is a global, monotonic version that
  *     every delivery instance agrees on. It is used as the SSE event id.
  *   - Compaction by [[ChangeEvent.compactionKey]] keeps only the latest state of every flag and segment, so a
  *     consumer can rebuild the whole state by reading the topic from the beginning.
  */
object ChangeFeed:

  def partition(config: KafkaConfig): TopicPartition = TopicPartition(config.topic, 0)

  /** Creates the topic if it does not exist yet. Safe to call from every service on startup. */
  def ensureTopic[F[_]: Async](config: KafkaConfig): F[Unit] =
    KafkaAdminClient
      .resource[F](AdminClientSettings(config.bootstrapServers))
      .use: admin =>
        val topic = NewTopic(config.topic, 1, config.replicationFactor).configs(
          Map(
            "cleanup.policy"            -> "compact",
            "min.compaction.lag.ms"     -> "0",
            "segment.ms"                -> (6 * 60 * 60 * 1000).toString,
            "min.cleanable.dirty.ratio" -> "0.1"
          ).asJava
        )
        admin.createTopic(topic).recover { case e if isTopicExists(e) => () }

  private def isTopicExists(e: Throwable): Boolean =
    e.isInstanceOf[TopicExistsException] || Option(e.getCause).exists(_.isInstanceOf[TopicExistsException])

  def encode(event: ChangeEvent): String = event.asJson.noSpaces

  def decodeEvent(value: String): Either[String, ChangeEvent] =
    decode[ChangeEvent](value).leftMap(_.getMessage)

  /** Idempotent producer with `acks=all`: no duplicates or reordering caused by producer retries. */
  def producerSettings[F[_]: Async](config: KafkaConfig): ProducerSettings[F, String, String] =
    ProducerSettings[F, String, String]
      .withBootstrapServers(config.bootstrapServers)
      .withAcks(Acks.All)
      .withEnableIdempotence(true)

  /** Settings for consumers that read the whole feed themselves (manual assignment, no offset commits). */
  def consumerSettings[F[_]: Async](config: KafkaConfig): ConsumerSettings[F, String, String] =
    ConsumerSettings[F, String, String]
      .withBootstrapServers(config.bootstrapServers)
      .withEnableAutoCommit(false)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
