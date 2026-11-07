package flagforge.delivery

import cats.data.NonEmptySet
import cats.effect.Async
import cats.syntax.all.*
import flagforge.messaging.{ChangeFeed, KafkaConfig}
import fs2.concurrent.SignallingRef
import fs2.kafka.KafkaConsumer
import fs2.kafka.instances.fs2KafkaTopicPartitionOrder
import flagforge.telemetry.Telemetry
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{BucketBoundaries, Counter, Histogram}

import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.*

/** Follows the change feed and folds it into [[DeliveryState]].
  *
  * Starts right after the offset already in the state (restored from a snapshot, or reached before a
  * restart); `ready` turns true once the consumer has caught up with the end of the topic as it was on
  * startup.
  */
final class FeedConsumer[F[_]: Async: LoggerFactory](
    kafka: KafkaConfig,
    state: SignallingRef[F, DeliveryState],
    ready: SignallingRef[F, Boolean],
    telemetry: Telemetry[F]
):
  private val logger = LoggerFactory[F].getLogger

  /** What happened to one consumed record, for telemetry. */
  private final case class Outcome(
      offset: Long,
      eventType: String,
      result: String,
      headers: Map[String, String]
  ):
    def createdAt: Option[Long] = headers.get(CreatedAtHeader).flatMap(_.toLongOption)

  private val CreatedAtHeader = "flagforge-created-at"

  /** For every applied change: a span from the commit in admin-api to now, continuing the trace of the
    * request that made the change (so one trace shows the whole journey), a propagation latency sample and a
    * counter.
    */
  private def observe(
      outcomes: List[Outcome],
      events: Counter[F, Long],
      latency: Histogram[F, Double]
  ): F[Unit] =
    Async[F].realTime.flatMap: now =>
      outcomes.traverse_ : o =>
        val attributes = List(Attribute("event.type", o.eventType), Attribute("outcome", o.result))
        val span       = telemetry.tracer.joinOrRoot(o.headers):
          telemetry.tracer
            .spanBuilder("change-feed apply")
            .modifyState(s =>
              o.createdAt.fold(s)(t => s.withStartTimestamp(t.millis))
                .addAttributes(attributes :+ Attribute("messaging.kafka.offset", o.offset))
            )
            .build
            .use_
        val sample = o.createdAt.filter(_ => o.result == "applied").traverse_(t =>
          latency.record((now.toMillis - t) / 1000.0)
        )
        events.add(1, attributes*) >> sample >> span

  def run: F[Unit] =
    KafkaConsumer.resource(ChangeFeed.consumerSettings[F](kafka)).use: consumer =>
      val tp = ChangeFeed.partition(kafka)
      for
        events <- telemetry.meter
          .counter[Long]("flagforge.delivery.events")
          .withDescription("Change events consumed, by type and outcome (applied, stale, invalid)")
          .create
        latency <- telemetry.meter
          .histogram[Double]("flagforge.delivery.propagation")
          .withUnit("s")
          .withDescription("Time from commit in admin-api to the change being applied in flag-delivery")
          .withExplicitBucketBoundaries(BucketBoundaries(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
          .create
        _       <- consumer.assign(NonEmptySet.one(tp))
        end     <- consumer.endOffsets(Set(tp)).map(_.getOrElse(tp, 0L))
        current <- state.get
        start   <-
          if current.offset >= end then
            // The topic is behind our state: it was recreated. Our state describes a feed that no longer exists.
            logger.warn(
              s"State offset ${current.offset} is ahead of the topic end $end, rebuilding from scratch"
            ) >>
              state.set(DeliveryState.empty(current.maxRecent)).as(0L)
          else (current.offset + 1).pure[F]
        _ <- consumer.seek(tp, start)
        _ <- ready.set(true).whenA(start >= end)
        _ <- logger.info(s"Consuming ${kafka.topic} from offset $start, end offset at startup: $end")
        _ <- consumer.stream.chunks
          .evalMap: chunk =>
            state
              .modify: s =>
                val (next, errors, outcomes) = chunk.foldLeft((s, List.empty[String], List.empty[Outcome])):
                  case ((acc, errs, outs), committable) =>
                    val record  = committable.record
                    val headers =
                      record.headers.toChain.toList.map(h => h.key -> String(h.value, UTF_8)).toMap
                    Option(record.value).toRight("tombstone").flatMap(ChangeFeed.decodeEvent) match
                      case Right(event) =>
                        val after  = acc.applied(record.offset, event)
                        val result = if after.recent.lastOption.exists(_.offset == record.offset) then
                          "applied"
                        else "stale"
                        val outcome = Outcome(record.offset, event.productPrefix, result, headers)
                        (after, errs, outcome :: outs)
                      case Left(error) =>
                        val outcome = Outcome(record.offset, "unknown", "invalid", headers)
                        (
                          acc.skipped(record.offset),
                          s"offset ${record.offset}: $error" :: errs,
                          outcome :: outs
                        )
                (next, (next.offset, errors.reverse, outcomes.reverse))
              .flatMap: (offset, errors, outcomes) =>
                errors.traverse_(e => logger.warn(s"Skipped undecodable change event at $e")) >>
                  observe(outcomes, events, latency) >>
                  ready.set(true).whenA(offset >= end - 1)
          .compile
          .drain
      yield ()
