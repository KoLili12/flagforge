package flagforge.sdk

import cats.effect.std.{Queue, UUIDGen}
import cats.effect.syntax.all.*
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all.*
import flagforge.domain.*
import fs2.{Chunk, Stream}
import io.circe.syntax.*
import org.http4s.*
import org.http4s.client.Client
import org.http4s.headers.{`Content-Type`, `Retry-After`}
import org.typelevel.ci.*
import org.typelevel.log4cats.LoggerFactory

import scala.concurrent.duration.*

/** Settings of analytics delivery. Without `uri` nothing is sent. */
final case class EventsConfig(
    uri: Option[Uri] = None,
    flushInterval: FiniteDuration = 2.seconds,
    maxBatch: Int = 500,
    /** Events beyond this are dropped rather than slowing the application down. */
    maxQueued: Int = 10_000,
    maxAttempts: Int = 4
)

/** Where the client records what users saw and did. */
private[sdk] trait EventSink[F[_]]:
  def exposed(exposure: Exposure): F[Unit]
  def tracked(event: MetricEvent): F[Unit]

private[sdk] object EventSink:

  def noop[F[_]: Async]: EventSink[F] = new:
    def exposed(exposure: Exposure): F[Unit] = Async[F].unit
    def tracked(event: MetricEvent): F[Unit] = Async[F].unit

  private enum Outbound:
    case Exposed(exposure: Exposure)
    case Tracked(event: MetricEvent)

  /** Asynchronous delivery to the ingest service:
    *
    *   - a bounded queue filled with `tryOffer`: recording an event never blocks and never fails;
    *   - exposures are deduplicated locally (one per flag, variation and context) to save traffic;
    *   - batches are flushed by size or time, retried with backoff (honouring `Retry-After` on 429);
    *   - whatever is queued is flushed when the client is closed.
    */
  def http[F[_]: Async: LoggerFactory: UUIDGen](
      client: Client[F],
      environment: EnvironmentKey,
      config: EventsConfig,
      uri: Uri
  ): Resource[F, EventSink[F]] =
    val logger   = LoggerFactory[F].getLogger
    val endpoint = uri / "events" / "v1" / "environments" / environment.value / "batch"
    val maxSeen  = 100_000

    def send(clientId: String)(events: Chunk[Outbound]): F[Unit] =
      val batch = EventBatch(
        events.toList.collect { case Outbound.Exposed(e) => e },
        events.toList.collect { case Outbound.Tracked(m) => m }
      )
      val request = Request[F](Method.POST, endpoint)
        .putHeaders(Header.Raw(ci"X-Client-Id", clientId))
        .withEntity(batch.asJson.noSpaces)
        .withContentType(`Content-Type`(MediaType.application.json))

      def attempt(n: Int): F[Unit] =
        val retry: Option[FiniteDuration] => F[Unit] = hint =>
          if n >= config.maxAttempts then logger.warn(s"Dropping ${batch.size} analytics events after $n attempts")
          else Async[F].sleep(hint.getOrElse((200L << n).millis)) >> attempt(n + 1)
        client
          .run(request)
          .use: response =>
            response.status match
              case s if s.isSuccess => Async[F].unit
              case Status.TooManyRequests =>
                retry(response.headers.get[`Retry-After`].flatMap(_.retry.toOption).map(_.seconds))
              case s if s.responseClass == Status.ClientError =>
                response.as[String].flatMap(body => logger.warn(s"Analytics events rejected ($s): $body"))
              case _ => retry(None)
          .handleErrorWith(e => logger.debug(e)("Sending analytics events failed") >> retry(None))

      if batch.isEmpty then Async[F].unit else attempt(1)

    for
      clientId <- Resource.eval(UUIDGen[F].randomUUID.map(_.toString))
      queue    <- Resource.eval(Queue.bounded[F, Option[Outbound]](config.maxQueued))
      seen     <- Resource.eval(Ref.of[F, Set[String]](Set.empty))
      // Graceful shutdown: `None` goes through the same queue, after every event recorded before it. The stream then
      // ends, `groupWithin` emits the batch it was accumulating, and the fiber completes once it has been sent.
      // (Cancelling the fiber instead would lose that batch.)
      _ <- Resource.make(
        Stream
          .fromQueueNoneTerminated(queue)
          .groupWithin(config.maxBatch, config.flushInterval)
          .evalMap(send(clientId))
          .compile
          .drain
          .start
      ): fiber =>
        (queue.offer(None) >> fiber.join.void)
          .timeoutTo(30.seconds, logger.warn("Analytics flush timed out") >> fiber.cancel)
          .handleErrorWith(e => logger.warn(e)("Final analytics flush failed"))
    yield new EventSink[F]:
      def exposed(exposure: Exposure): F[Unit] =
        val key = s"${exposure.flag}/${exposure.variation}/${exposure.contextKey}"
        seen
          .modify(s => if s.contains(key) then (s, false) else ((if s.size >= maxSeen then Set.empty else s) + key, true))
          .ifM(enqueue(Outbound.Exposed(exposure)), Async[F].unit)

      def tracked(event: MetricEvent): F[Unit] = enqueue(Outbound.Tracked(event))

      /** Never blocks: when the queue is full (ingest down for a long time) the event is dropped. */
      private def enqueue(event: Outbound): F[Unit] = queue.tryOffer(Some(event)).void
