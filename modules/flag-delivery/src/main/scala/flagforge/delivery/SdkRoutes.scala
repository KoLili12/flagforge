package flagforge.delivery

import cats.effect.{Ref, Temporal}
import cats.syntax.all.*
import flagforge.domain.*
import flagforge.eval.Evaluator
import fs2.Stream
import fs2.concurrent.Signal
import io.circe.Json
import io.circe.syntax.*
import org.http4s.*
import org.http4s.CacheDirective.`no-cache`
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{`Cache-Control`, `If-None-Match`, `Last-Event-Id`, ETag}
import org.typelevel.ci.*

import scala.concurrent.duration.FiniteDuration

/** SDK-facing HTTP API.
  *
  *   - `GET  /sdk/v1/environments/{env}/stream` — Server-Sent Events (`put` / `patch`, heartbeats)
  *   - `GET  /sdk/v1/environments/{env}/ruleset` — snapshot for polling clients, with `ETag`
  *   - `POST /sdk/v1/environments/{env}/evaluate` — server-side evaluation for thin clients
  *   - `GET  /health`, `GET /ready`
  */
final class SdkRoutes[F[_]: Temporal](
    state: Signal[F, DeliveryState],
    ready: Signal[F, Boolean],
    connections: Ref[F, Int],
    heartbeat: FiniteDuration
) extends Http4sDsl[F]:

  private object Env:
    def unapply(s: String): Option[EnvironmentKey] = EnvironmentKey.option(s)

  private val sdk = Root / "sdk" / "v1" / "environments"

  val routes: HttpRoutes[F] = HttpRoutes.of[F]:
    case GET -> Root / "health" => Ok("ok")

    case GET -> Root / "ready" =>
      (ready.get, state.get, connections.get).flatMapN: (isReady, s, clients) =>
        val body = Json.obj(
          "ready"        -> isReady.asJson,
          "offset"       -> s.offset.asJson,
          "environments" -> s.environments.size.asJson,
          "connections"  -> clients.asJson
        )
        if isReady then Ok(body) else ServiceUnavailable(body)

    case req @ GET -> `sdk` / Env(env) / "stream" =>
      whenReady:
        Ok(
          stream(env, req.headers.get[`Last-Event-Id`].flatMap(_.id.value.toLongOption)),
          `Cache-Control`(`no-cache`()),
          Header.Raw(ci"X-Accel-Buffering", "no")
        )

    case req @ GET -> `sdk` / Env(env) / "ruleset" =>
      whenReady:
        state.get.flatMap: s =>
          val tag   = EntityTag(s.offset.toString)
          val fresh = req.headers.get[`If-None-Match`].flatMap(_.tags).exists(_.exists(_.tag == tag.tag))
          if fresh then NotModified() else Ok(s.ruleset(env), ETag(tag))

    case req @ POST -> `sdk` / Env(env) / "evaluate" =>
      whenReady:
        req.as[EvaluationContext].flatMap: context =>
          state.get.flatMap(s =>
            Ok(Evaluator.evaluateAll(s.ruleset(env), context).values.toList.sortBy(_.flag))
          )

  private def whenReady(response: => F[Response[F]]): F[Response[F]] =
    ready.get.ifM(response, ServiceUnavailable("change feed is still loading"))

  /** Every client follows the state signal on its own. `discrete` skips intermediate states for a slow client
    * (it then gets a `put` if it fell out of the replay window), so one slow client never blocks the others.
    */
  def stream(env: EnvironmentKey, lastSeen: Option[Long]): Stream[F, ServerSentEvent] =
    val updates = state.discrete
      .mapAccumulate(lastSeen)((cursor, s) => Outgoing.plan(env, cursor, s))
      .flatMap((_, messages) => Stream.emits(messages.map(_.toSse)))
    val heartbeats = Stream.awakeEvery[F](heartbeat).as(ServerSentEvent(comment = Some("heartbeat")))
    Stream.bracket(connections.update(_ + 1))(_ => connections.update(_ - 1)) >> updates.merge(heartbeats)
