package flagforge.sdk

import cats.Eq
import cats.effect.std.Random
import cats.effect.syntax.all.*
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all.*
import flagforge.domain.*
import fs2.Stream
import fs2.concurrent.{Signal, SignallingRef}
import fs2.io.net.Network
import io.circe.Decoder
import io.circe.parser.decode
import org.http4s.*
import org.http4s.ServerSentEvent.EventId
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.{`Last-Event-Id`, Accept, Connection}
import org.typelevel.log4cats.LoggerFactory

import scala.concurrent.duration.*

final case class SdkConfig(
    baseUri: Uri,
    environment: EnvironmentKey,
    minBackoff: FiniteDuration = 250.millis,
    maxBackoff: FiniteDuration = 30.seconds,
    /** No bytes for this long (the server sends heartbeats every 15s) means the connection is dead. */
    idleTimeout: FiniteDuration = 45.seconds
)

enum ConnectionState:
  case Connecting
  case Connected
  case Disconnected(reason: String, retryIn: FiniteDuration)

/** Feature flag client.
  *
  * Keeps the environment's ruleset in memory, updated in real time over Server-Sent Events, and evaluates
  * flags locally: `get` never makes a network call and never fails. On disconnect it reconnects with
  * exponential backoff and full jitter, sending `Last-Event-ID` to receive only the missed changes.
  */
trait FlagforgeClient[F[_]]:
  def evaluate[A](flag: Flag[A], context: EvaluationContext): F[FlagResult[A]]
  def get[A](flag: Flag[A], context: EvaluationContext): F[A]

  /** Current value, then every change of it. */
  def watch[A](flag: Flag[A], context: EvaluationContext): Stream[F, A]

  /** Waits for the first ruleset. `false` on timeout (flags then evaluate to their defaults). */
  def awaitReady(timeout: FiniteDuration): F[Boolean]

  def ruleset: Signal[F, Option[Ruleset]]
  def connection: Signal[F, ConnectionState]

object FlagforgeClient:

  def resource[F[_]: Async: Network: LoggerFactory](config: SdkConfig): Resource[F, FlagforgeClient[F]] =
    EmberClientBuilder
      .default[F]
      .withIdleConnectionTime(config.idleTimeout)
      .withTimeout(10.seconds) // time to receive response headers
      .build
      .flatMap(fromHttpClient(_, config))

  def fromHttpClient[F[_]: Async: LoggerFactory](
      http: Client[F],
      config: SdkConfig
  ): Resource[F, FlagforgeClient[F]] =
    for
      rulesets    <- Resource.eval(SignallingRef.of[F, Option[Ruleset]](None))
      connState   <- Resource.eval(SignallingRef.of[F, ConnectionState](ConnectionState.Connecting))
      lastEventId <- Resource.eval(Ref.of[F, Option[EventId]](None))
      failures    <- Resource.eval(Ref.of[F, Int](0))
      random      <- Resource.eval(Random.scalaUtilRandom[F])
      streaming = Streaming(http, config, rulesets, connState, lastEventId, failures, random)
      _ <- streaming.run.background
    yield new FlagforgeClient[F]:
      def evaluate[A](flag: Flag[A], context: EvaluationContext): F[FlagResult[A]] =
        rulesets.get.map(LocalEvaluation.evaluate(_, flag, context))

      def get[A](flag: Flag[A], context: EvaluationContext): F[A] = evaluate(flag, context).map(_.value)

      def watch[A](flag: Flag[A], context: EvaluationContext): Stream[F, A] =
        rulesets.discrete.map(LocalEvaluation.evaluate(_, flag, context).value).changes(using
          Eq.fromUniversalEquals
        )

      def awaitReady(timeout: FiniteDuration): F[Boolean] =
        rulesets.discrete.exists(_.isDefined).compile.lastOrError.timeoutTo(timeout, false.pure[F])

      def ruleset: Signal[F, Option[Ruleset]]    = rulesets
      def connection: Signal[F, ConnectionState] = connState

  private final class Streaming[F[_]: Async: LoggerFactory](
      http: Client[F],
      config: SdkConfig,
      rulesets: SignallingRef[F, Option[Ruleset]],
      connection: SignallingRef[F, ConnectionState],
      lastEventId: Ref[F, Option[EventId]],
      failures: Ref[F, Int],
      random: Random[F]
  ):
    private val logger = LoggerFactory[F].getLogger

    private val streamUri =
      config.baseUri / "sdk" / "v1" / "environments" / config.environment.value / "stream"

    private object StreamEnded extends RuntimeException("stream closed by the server")

    def run: F[Unit] = (session ++ Stream.raiseError[F](StreamEnded))
      .handleErrorWith(e => Stream.exec(backoff(e)))
      .repeat
      .compile
      .drain

    private def session: Stream[F, Unit] =
      Stream.eval(lastEventId.get).flatMap: lastId =>
        val request = Request[F](Method.GET, streamUri)
          .putHeaders(Accept(MediaType.`text/event-stream`))
          // A stream is never reusable; without this ember would try to drain it on close.
          .putHeaders(Connection.close)
          .putHeaders(lastId.map(`Last-Event-Id`(_)).toList)
        http
          .stream(request)
          .flatMap: response =>
            if response.status == Status.Ok then
              Stream.exec(connection.set(ConnectionState.Connected)) ++
                response.body.through(ServerSentEvent.decoder[F]).evalMap(handle)
            else Stream.raiseError[F](RuntimeException(s"unexpected status ${response.status}"))

    private def handle(event: ServerSentEvent): F[Unit] =
      val applied = event.eventType match
        case Some("put")   => parse[Ruleset](event)(rs => rulesets.set(Some(rs)))
        case Some("patch") =>
          parse[ChangeEvent](event)(c => rulesets.update(_.map(rs => rs.applyChange(c).getOrElse(rs))))
        case _ => ().pure[F] // heartbeat
      applied >> failures.set(0)

    private def parse[A: Decoder](event: ServerSentEvent)(apply: A => F[Unit]): F[Unit] =
      decode[A](event.data.getOrElse("")) match
        case Right(a) => apply(a) >> event.id.traverse_(id => lastEventId.set(Some(id)))
        case Left(e)  =>
          logger.warn(s"Ignoring malformed '${event.eventType.mkString}' event: ${e.getMessage}")

    /** Exponential backoff with full jitter: random delay in [min, min * 2^failures], capped. */
    private def backoff(error: Throwable): F[Unit] =
      for
        attempt <- failures.getAndUpdate(_ + 1)
        ceiling = (config.minBackoff * math.pow(2, attempt.min(20).toDouble).toLong).min(config.maxBackoff)
        jitter <-
          random.betweenLong(config.minBackoff.toMillis, ceiling.toMillis.max(config.minBackoff.toMillis + 1))
        delay = jitter.millis
        _ <- connection.set(ConnectionState.Disconnected(error.getMessage, delay))
        _ <- logger.info(s"Flag stream disconnected (${error.getMessage}), reconnecting in $delay")
        _ <- Async[F].sleep(delay)
        _ <- connection.set(ConnectionState.Connecting)
      yield ()
