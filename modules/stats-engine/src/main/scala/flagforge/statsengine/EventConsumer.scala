package flagforge.statsengine

import cats.effect.Async
import cats.syntax.all.*
import flagforge.messaging.{AnalyticsTopics, KafkaConfig}
import fs2.kafka.*
import org.typelevel.log4cats.LoggerFactory

import scala.concurrent.duration.*

/** Consumer group that stores analytics events. Offsets are committed only after a batch is written, and
  * writes are idempotent: at-least-once delivery without double counting. Instances share the partitions.
  */
final class EventConsumer[F[_]: Async: LoggerFactory](kafka: KafkaConfig, store: StatsStore[F]):
  private val logger = LoggerFactory[F].getLogger

  private val settings =
    ConsumerSettings[F, String, String]
      .withBootstrapServers(kafka.bootstrapServers)
      .withGroupId("flagforge-stats-engine")
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)

  def run: F[Unit] =
    KafkaConsumer
      .stream(settings)
      .subscribeTo(AnalyticsTopics.Exposures, AnalyticsTopics.Metrics)
      .records
      .groupWithin(2000, 250.millis)
      .evalMap: chunk =>
        val records                                                        = chunk.toList
        def decoded[A](topic: String, decode: String => Either[String, A]) =
          records.filter(_.record.topic == topic).map(r =>
            decode(r.record.value).leftMap(e => s"${r.offset}: $e")
          )
        val exposures = decoded(AnalyticsTopics.Exposures, AnalyticsTopics.decodeExposure)
        val metrics   = decoded(AnalyticsTopics.Metrics, AnalyticsTopics.decodeMetric)
        val errors    = exposures.collect { case Left(e) => e } ++ metrics.collect { case Left(e) => e }
        for
          _ <- store.insertExposures(exposures.collect { case Right(e) => e })
          _ <- store.insertMetrics(metrics.collect { case Right(m) => m })
          _ <- errors.traverse_(e => logger.warn(s"Skipped undecodable analytics event at $e"))
          _ <- CommittableOffsetBatch.fromFoldable(chunk.map(_.offset)).commit
        yield ()
      .compile
      .drain
