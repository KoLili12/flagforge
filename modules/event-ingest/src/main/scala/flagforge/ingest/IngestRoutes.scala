package flagforge.ingest

import cats.effect.Concurrent
import cats.syntax.all.*
import flagforge.domain.*
import io.circe.Json
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Retry-After`
import org.typelevel.ci.*

/** `POST /events/v1/environments/{env}/batch` with an [[EventBatch]] body.
  *
  * Clients identify themselves with `X-Client-Id` (the SDK sends a random id per instance); rate limits are per
  * client and environment. Responses: 202 (with accepted/duplicate counts), 400, 429 + `Retry-After`, 503.
  */
final class IngestRoutes[F[_]: Concurrent](service: IngestService[F]) extends Http4sDsl[F]:

  private object Env:
    def unapply(s: String): Option[EnvironmentKey] = EnvironmentKey.option(s)

  val routes: HttpRoutes[F] = HttpRoutes.of[F]:
    case GET -> Root / "health" => Ok("ok")

    case req @ POST -> Root / "events" / "v1" / "environments" / Env(env) / "batch" =>
      val client = req.headers
        .get(ci"X-Client-Id")
        .map(_.head.value)
        .orElse(req.remoteAddr.map(_.toString))
        .getOrElse("anonymous")
      req
        .attemptAs[EventBatch]
        .foldF(
          failure => BadRequest(Json.obj("errors" -> List(failure.message).asJson)),
          batch =>
            service
              .ingest(env, client, batch)
              .flatMap:
                case IngestResult.Accepted(accepted, duplicates) =>
                  Accepted(Json.obj("accepted" -> accepted.asJson, "duplicates" -> duplicates.asJson))
                case IngestResult.Invalid(errors) => BadRequest(Json.obj("errors" -> errors.asJson))
                case IngestResult.RateLimited(retryAfter) =>
                  TooManyRequests(
                    Json.obj("error" -> "rate limit exceeded".asJson),
                    `Retry-After`.unsafeFromLong(math.max(1L, retryAfter.toSeconds + 1))
                  )
              .handleErrorWith(_ => ServiceUnavailable(Json.obj("error" -> "could not store events, retry".asJson)))
        )
