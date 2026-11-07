package flagforge.messaging

import cats.effect.Async
import cats.syntax.all.*
import fs2.kafka.{AdminClientSettings, KafkaAdminClient}
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException

object Topics:

  /** Creates the topics that do not exist yet. Safe to call concurrently from every service on startup. */
  def ensure[F[_]: Async](bootstrapServers: String, topics: NewTopic*): F[Unit] =
    KafkaAdminClient
      .resource[F](AdminClientSettings(bootstrapServers))
      .use: admin =>
        topics.toList.traverse_(t => admin.createTopic(t).recover { case e if isTopicExists(e) => () })

  private def isTopicExists(e: Throwable): Boolean =
    e.isInstanceOf[TopicExistsException] || Option(e.getCause).exists(_.isInstanceOf[TopicExistsException])
