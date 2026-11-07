package flagforge.ingest

import cats.Monad
import cats.syntax.all.*
import flagforge.domain.EventBatch
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{BucketBoundaries, Counter, Histogram, Meter}

/** `flagforge.ingest.*` metrics: what happens to incoming analytics events. */
final class IngestMetrics[F[_]: Monad](
    events: Counter[F, Long],
    requestsRejected: Counter[F, Long],
    batchSize: Histogram[F, Double]
):
  def observe(batch: EventBatch, result: IngestResult): F[Unit] =
    batchSize.record(batch.size.toDouble) >>
      (result match
        case IngestResult.Accepted(accepted, duplicates) =>
          events.add(accepted.toLong, Attribute("outcome", "accepted")) >>
            events.add(duplicates.toLong, Attribute("outcome", "duplicate"))
        case IngestResult.Invalid(_)     => rejected("invalid")
        case IngestResult.RateLimited(_) => rejected("rate_limited"))

  def rejected(reason: String): F[Unit] = requestsRejected.add(1, Attribute("reason", reason))

object IngestMetrics:
  def make[F[_]: Monad](meter: Meter[F]): F[IngestMetrics[F]] =
    (
      meter.counter[Long]("flagforge.ingest.events").withDescription("Analytics events by outcome").create,
      meter.counter[Long]("flagforge.ingest.rejected").withDescription("Rejected batches by reason").create,
      meter
        .histogram[Double]("flagforge.ingest.batch.size")
        .withDescription("Events per batch")
        .withExplicitBucketBoundaries(BucketBoundaries(1, 5, 10, 25, 50, 100, 250, 500, 1000))
        .create
    ).mapN(IngestMetrics(_, _, _))
