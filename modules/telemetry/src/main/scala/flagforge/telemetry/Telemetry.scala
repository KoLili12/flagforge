package flagforge.telemetry

import cats.Applicative
import cats.data.{Kleisli, OptionT}
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import org.http4s.otel4s.middleware.metrics.OtelMetrics
import org.http4s.otel4s.middleware.trace.redact.{PathRedactor, QueryRedactor}
import org.http4s.otel4s.middleware.trace.server.{ServerMiddleware, ServerSpanDataProvider}
import org.http4s.otel4s.middleware.trace.{PerRequestConfig, PerRequestFilter}
import org.http4s.server.middleware.Metrics
import org.http4s.{HttpApp, HttpRoutes, Uri}
import org.typelevel.otel4s.metrics.{Meter, MeterProvider}
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.oteljava.context.LocalContextProvider
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

import scala.jdk.CollectionConverters.*

/** Tracing and metrics of one service. */
final case class Telemetry[F[_]](
    tracerProvider: TracerProvider[F],
    meterProvider: MeterProvider[F],
    tracer: Tracer[F],
    meter: Meter[F]
)

object Telemetry:

  /** Everything is a no-op: used in tests and when no collector is configured. */
  def noop[F[_]: Applicative]: Telemetry[F] =
    Telemetry(TracerProvider.noop[F], MeterProvider.noop[F], Tracer.noop[F], Meter.noop[F])

  /** OpenTelemetry Java SDK configured with the standard `OTEL_*` environment variables
    * (https://opentelemetry.io/docs/languages/java/configuration/).
    *
    * Telemetry is exported only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set (or `OTEL_SDK_DISABLED=false`);
    * otherwise it is a no-op, so running a service locally needs no collector and produces no export errors.
    */
  def autoConfigured[F[_]: Async: LocalContextProvider](serviceName: String): Resource[F, Telemetry[F]] =
    val env     = sys.env
    val enabled = env.contains("OTEL_EXPORTER_OTLP_ENDPOINT") ||
      env.get("OTEL_SDK_DISABLED").contains("false")
    if !enabled then Resource.pure(noop[F])
    else
      // Lowest-priority defaults: any OTEL_* environment variable overrides them.
      val defaults = Map(
        "otel.service.name"           -> serviceName,
        "otel.logs.exporter"          -> "none",
        "otel.metric.export.interval" -> "5000",
        "otel.bsp.schedule.delay"     -> "1000"
      ).asJava
      OtelJava
        .autoConfigured[F](_.addPropertiesSupplier(() => defaults))
        .evalMap: otel =>
          (otel.tracerProvider.get("flagforge"), otel.meterProvider.get("flagforge")).mapN: (tracer, meter) =>
            Telemetry(otel.tracerProvider, otel.meterProvider, tracer, meter)

  /** Server spans (W3C trace context is continued from incoming requests) and `http.server.*` metrics. Health
    * and readiness probes, and paths matching `skipTracing` (e.g. long-lived streams), are not traced.
    */
  def instrument[F[_]: Async](telemetry: Telemetry[F], skipTracing: Uri.Path => Boolean = _ => false)(
      app: HttpApp[F]
  ): F[HttpApp[F]] =
    val filter: PerRequestFilter = request =>
      val path = request.uri.path
      if Set("/health", "/ready").contains(path.renderString) || skipTracing(path) then
        PerRequestConfig.Disabled
      else PerRequestConfig.Enabled
    given TracerProvider[F] = telemetry.tracerProvider
    given MeterProvider[F]  = telemetry.meterProvider
    val redactor            = new PathRedactor.NeverRedact with QueryRedactor.NeverRedact {}
    for
      metricsOps <- OtelMetrics.serverMetricsOps[F]()
      tracing    <- ServerMiddleware
        .builder[F](ServerSpanDataProvider.openTelemetry(redactor))
        .withPerRequestTracingFilter(filter)
        .build
    yield
      val routes: HttpRoutes[F] = Kleisli(request => OptionT.liftF(app(request)))
      tracing.wrapHttpApp(Metrics[F](metricsOps)(routes).orNotFound)
