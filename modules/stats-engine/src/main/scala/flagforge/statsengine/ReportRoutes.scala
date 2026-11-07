package flagforge.statsengine

import cats.effect.Concurrent
import cats.syntax.all.*
import flagforge.domain.*
import flagforge.stats.Analysis
import io.circe.Json
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.dsl.Http4sDsl

/** Experiment reports.
  *
  *   - `GET /stats/v1/environments/{env}/flags/{flag}` — exposures per variation and the available metrics
  *   - `GET /stats/v1/environments/{env}/flags/{flag}/metrics/{metric}` — full report; query parameters:
  *     `control` (default: `control`, then `off`, then the first variation), `weights` (configured split, e.g.
  *     `on:50,off:50`, for the sample ratio mismatch check; default: equal), `alpha` (default 0.05)
  */
final class ReportRoutes[F[_]: Concurrent](store: StatsStore[F]) extends Http4sDsl[F]:

  private object Env:
    def unapply(s: String): Option[EnvironmentKey] = EnvironmentKey.option(s)
  private object Flag:
    def unapply(s: String): Option[FlagKey] = FlagKey.option(s)
  private object Metric:
    def unapply(s: String): Option[MetricKey] = MetricKey.option(s)

  private object ControlParam extends OptionalQueryParamDecoderMatcher[String]("control")
  private object WeightsParam extends OptionalQueryParamDecoderMatcher[String]("weights")
  private object AlphaParam   extends OptionalQueryParamDecoderMatcher[Double]("alpha")

  private val base = Root / "stats" / "v1" / "environments"

  val routes: HttpRoutes[F] = HttpRoutes.of[F]:
    case GET -> Root / "health" => Ok("ok")

    case GET -> `base` / Env(env) / "flags" / Flag(flag) =>
      (store.exposureCounts(env, flag), store.metrics(env, flag)).flatMapN: (counts, metrics) =>
        Ok(Json.obj("exposures" -> counts.asJson, "metrics" -> metrics.asJson))

    case GET -> `base` / Env(env) / "flags" / Flag(flag) / "metrics" / Metric(metric)
        :? ControlParam(control) +& WeightsParam(weights) +& AlphaParam(alpha) =>
      parseWeights(weights) match
        case Left(error) => BadRequest(Json.obj("error" -> error.asJson))
        case Right(expected) =>
          store.variationStats(env, flag, metric).flatMap: stats =>
            if stats.isEmpty then NotFound(Json.obj("error" -> s"no exposures for '$flag' in '$env'".asJson))
            else
              val variations = stats.map(_.variation)
              val chosen = control
                .orElse(List("control", "off").find(variations.contains))
                .getOrElse(variations.min)
              Analysis.report(stats, chosen, expected, alpha.getOrElse(0.05)) match
                case Right(report) => Ok(report)
                case Left(error)   => BadRequest(Json.obj("error" -> error.asJson))

  /** `on:50,off:50` → Map(on -> 50.0, off -> 50.0) */
  private def parseWeights(raw: Option[String]): Either[String, Map[String, Double]] =
    raw.filter(_.nonEmpty).fold(Map.empty[String, Double].asRight[String]): s =>
      s.split(',').toList
        .traverse: pair =>
          pair.split(':') match
            case Array(variation, weight) =>
              weight.toDoubleOption.filter(_ >= 0).map(variation -> _).toRight(s"bad weight in '$pair'")
            case _ => Left(s"expected variation:weight, got '$pair'")
        .map(_.toMap)
