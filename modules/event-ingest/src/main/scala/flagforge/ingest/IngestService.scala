package flagforge.ingest

import cats.MonadThrow
import cats.effect.Clock
import cats.syntax.all.*
import flagforge.domain.*
import flagforge.messaging.AnalyticsTopics
import fs2.Chunk
import fs2.kafka.KafkaProducer

import scala.concurrent.duration.*

/** Where accepted events go. */
trait EventPublisher[F[_]]:
  def publish(exposures: List[Exposure], metrics: List[MetricEvent]): F[Unit]

object EventPublisher:
  /** Waits for broker acknowledgements: a 202 to the SDK means the events are durable in Kafka. */
  def kafka[F[_]: MonadThrow](producer: KafkaProducer[F, String, String]): EventPublisher[F] = new:
    def publish(exposures: List[Exposure], metrics: List[MetricEvent]): F[Unit] =
      val records = exposures.map(AnalyticsTopics.record) ++ metrics.map(AnalyticsTopics.record)
      if records.isEmpty then ().pure[F] else producer.produce(Chunk.from(records)).flatten.void

enum IngestResult:
  case Accepted(accepted: Int, duplicates: Int)
  case Invalid(errors: List[String])
  case RateLimited(retryAfter: FiniteDuration)

final case class IngestLimits(maxBatch: Int = 1000, maxClockSkew: FiniteDuration = 5.minutes)

/** Validates, rate-limits and deduplicates a batch, then publishes what is new.
  *
  * Deduplication happens before publishing; if publishing fails, the keys are released again so that the SDK's
  * retry is not mistaken for a duplicate. Exposures are deduplicated per (environment, flag, context): only the first
  * one matters for analysis. Metric events are deduplicated by their client-generated id.
  */
final class IngestService[F[_]: MonadThrow: Clock](
    guards: Guards[F],
    publisher: EventPublisher[F],
    limits: IngestLimits
):
  def ingest(env: EnvironmentKey, clientKey: String, batch: EventBatch): F[IngestResult] =
    Clock[F].realTime.flatMap: now =>
      validate(env, batch, now.toMillis) match
        case errors if errors.nonEmpty => IngestResult.Invalid(errors).pure[F]
        case _ if batch.isEmpty        => IngestResult.Accepted(0, 0).pure[F]
        case _ =>
          guards.acquire(s"$env/$clientKey", batch.size).flatMap:
            case RateDecision.Limited(retryAfter) => IngestResult.RateLimited(retryAfter).pure[F]
            case RateDecision.Allowed             => deduplicateAndPublish(env, batch)

  private def deduplicateAndPublish(env: EnvironmentKey, batch: EventBatch): F[IngestResult] =
    // Inside one batch the SDK may repeat itself, too.
    val exposures = batch.exposures.distinctBy(_.dedupKey)
    val metrics   = batch.metrics.distinctBy(_.id)
    val keys      = exposures.map("exp:" + _.dedupKey) ++ metrics.map(m => s"met:$env/${m.id}")
    guards.firstSeen(keys).flatMap: fresh =>
      val (freshExposures, freshMetrics) = (fresh.take(exposures.size), fresh.drop(exposures.size))
      val newExposures = exposures.zip(freshExposures).collect { case (e, true) => e }
      val newMetrics   = metrics.zip(freshMetrics).collect { case (m, true) => m }
      val claimed      = keys.zip(fresh).collect { case (k, true) => k }
      publisher
        .publish(newExposures, newMetrics)
        .onError(_ => guards.forget(claimed))
        .as(IngestResult.Accepted(newExposures.size + newMetrics.size, batch.size - newExposures.size - newMetrics.size))

  private def validate(env: EnvironmentKey, batch: EventBatch, nowMillis: Long): List[String] =
    val latest = nowMillis + limits.maxClockSkew.toMillis
    List(
      Option.when(batch.size > limits.maxBatch)(s"batch has ${batch.size} events, at most ${limits.maxBatch} allowed"),
      Option.when((batch.exposures.map(_.environment) ++ batch.metrics.map(_.environment)).exists(_ != env))(
        s"all events must belong to environment '$env'"
      ),
      Option.when((batch.exposures.map(_.contextKey) ++ batch.metrics.map(_.contextKey)).exists(_.isEmpty))(
        "context keys must not be empty"
      ),
      Option.when(batch.metrics.exists(_.id.isEmpty))("metric event ids must not be empty"),
      Option.when((batch.exposures.map(_.timestamp) ++ batch.metrics.map(_.timestamp)).exists(_ > latest))(
        "timestamps must not be in the future"
      ),
      Option.when(batch.metrics.exists(_.value.exists(v => v.isNaN || v.isInfinite)))("metric values must be finite")
    ).flatten
