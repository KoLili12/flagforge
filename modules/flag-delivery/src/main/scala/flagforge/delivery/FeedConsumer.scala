package flagforge.delivery

import cats.data.NonEmptySet
import cats.effect.Async
import cats.syntax.all.*
import flagforge.messaging.{ChangeFeed, KafkaConfig}
import fs2.concurrent.SignallingRef
import fs2.kafka.KafkaConsumer
import fs2.kafka.instances.fs2KafkaTopicPartitionOrder
import org.typelevel.log4cats.LoggerFactory

/** Follows the change feed and folds it into [[DeliveryState]].
  *
  * Starts right after the offset already in the state (restored from a snapshot, or reached before a
  * restart); `ready` turns true once the consumer has caught up with the end of the topic as it was on
  * startup.
  */
final class FeedConsumer[F[_]: Async: LoggerFactory](
    kafka: KafkaConfig,
    state: SignallingRef[F, DeliveryState],
    ready: SignallingRef[F, Boolean]
):
  private val logger = LoggerFactory[F].getLogger

  def run: F[Unit] =
    KafkaConsumer.resource(ChangeFeed.consumerSettings[F](kafka)).use: consumer =>
      val tp = ChangeFeed.partition(kafka)
      for
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
                val (next, errors) = chunk.foldLeft((s, List.empty[String])):
                  case ((acc, errs), committable) =>
                    val record = committable.record
                    Option(record.value).toRight("tombstone").flatMap(ChangeFeed.decodeEvent) match
                      case Right(event) => (acc.applied(record.offset, event), errs)
                      case Left(error)  =>
                        (acc.skipped(record.offset), s"offset ${record.offset}: $error" :: errs)
                (next, (next.offset, errors.reverse))
              .flatMap: (offset, errors) =>
                errors.traverse_(e => logger.warn(s"Skipped undecodable change event at $e")) >>
                  ready.set(true).whenA(offset >= end - 1)
          .compile
          .drain
      yield ()
