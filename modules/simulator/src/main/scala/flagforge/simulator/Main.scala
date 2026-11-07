package flagforge.simulator

import cats.effect.std.Random
import cats.effect.{Async, IO, IOApp, Resource}
import cats.syntax.all.*
import ciris.*
import flagforge.domain.*
import flagforge.sdk.{EventsConfig, Flag, FlagforgeClient, SdkConfig}
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.ci.*
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.log4cats.{Logger, LoggerFactory}

import scala.concurrent.duration.*

final case class SimulatorConfig(
    adminUri: Uri,
    deliveryUri: Uri,
    ingestUri: Uri,
    statsUri: Uri,
    environment: EnvironmentKey,
    usersPerSecond: Int,
    userPool: Int,
    controlConversion: Double,
    treatmentConversion: Double,
    killSwitchEvery: FiniteDuration,
    reportEvery: FiniteDuration
)

object SimulatorConfig:
  private given ConfigDecoder[String, Uri] =
    ConfigDecoder[String].mapOption("Uri")(Uri.fromString(_).toOption)
  private given ConfigDecoder[String, EnvironmentKey] =
    ConfigDecoder[String].mapOption("EnvironmentKey")(EnvironmentKey.option)

  def load[F[_]: Async]: F[SimulatorConfig] =
    (
      env("ADMIN_URL").as[Uri].default(Uri.unsafeFromString("http://localhost:8080")),
      env("DELIVERY_URL").as[Uri].default(Uri.unsafeFromString("http://localhost:8081")),
      env("INGEST_URL").as[Uri].default(Uri.unsafeFromString("http://localhost:8083")),
      env("STATS_URL").as[Uri].default(Uri.unsafeFromString("http://localhost:8082")),
      env("ENVIRONMENT").as[EnvironmentKey].default(EnvironmentKey.applyUnsafe("demo")),
      env("USERS_PER_SECOND").as[Int].default(20),
      env("USER_POOL").as[Int].default(20_000),
      env("CONTROL_CONVERSION").as[Double].default(0.10),
      env("TREATMENT_CONVERSION").as[Double].default(0.12),
      env("KILL_SWITCH_EVERY").as[FiniteDuration].default(15.seconds),
      env("REPORT_EVERY").as[FiniteDuration].default(30.seconds)
    ).parMapN(SimulatorConfig.apply).load[F]

/** Drives the whole system like a real product would:
  *
  *   - sets up (idempotently) an environment, a 50/50 experiment `new-checkout` and a kill-switch flag
  *     `promo-banner` through the admin API;
  *   - simulates visitors through the real SDK: each visit evaluates the flags (recording exposures) and may
  *     end in a purchase, more likely in the `on` variation — the true effect the experiment should detect;
  *   - flips the kill switch periodically, which shows up as propagation latency samples and traces;
  *   - logs the experiment report from stats-engine.
  */
object Main extends IOApp.Simple:
  given LoggerFactory[IO] = Slf4jFactory.create[IO]
  private val logger      = LoggerFactory[IO].getLogger

  private val NewCheckout = Flag.bool("new-checkout", default = false)
  private val PromoBanner = Flag.bool("promo-banner", default = false)
  private val Purchase    = MetricKey("purchase")
  private val Countries   = Vector("RU", "KZ", "DE", "US", "BR", "IN")
  private val Plans       = Vector("free", "free", "free", "pro", "enterprise")

  def run: IO[Unit] =
    val app =
      for
        config <- SimulatorConfig.load[IO].toResource
        http   <- EmberClientBuilder.default[IO].withMaxTotal(64).build
        admin = Admin(http, config.adminUri, logger)
        _   <- Resource.eval(admin.setUp(config.environment))
        sdk <- FlagforgeClient.resource[IO](
          SdkConfig(
            config.deliveryUri,
            config.environment,
            events = EventsConfig(uri = Some(config.ingestUri))
          )
        )
        random <- Resource.eval(Random.scalaUtilRandom[IO])
      yield (config, http, admin, sdk, random)

    app.use: (config, http, admin, sdk, random) =>
      val visit: IO[Unit] =
        for
          userId  <- random.nextIntBounded(config.userPool)
          country <- random.elementOf(Countries)
          plan    <- random.elementOf(Plans)
          user = EvaluationContext(
            s"user-$userId",
            Map("country" -> Json.fromString(country), "plan" -> Json.fromString(plan))
          )
          newCheckout <- sdk.get(NewCheckout, user)
          _           <- sdk.get(PromoBanner, user)
          roll        <- random.nextDouble
          rate = if newCheckout then config.treatmentConversion else config.controlConversion
          amount <- random.nextGaussian.map(g =>
            BigDecimal(math.exp(3.5 + 0.6 * g)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
          )
          _ <- sdk.track(Purchase, user, Some(amount)).whenA(roll < rate)
        yield ()

      val traffic = Stream.awakeEvery[IO]((1.second / config.usersPerSecond.toLong)).evalMap(_ => visit)

      val killSwitch = Stream
        .awakeEvery[IO](config.killSwitchEvery)
        .zipWithIndex
        .evalMap((_, i) => admin.toggle(config.environment, PromoBanner.key, enabled = i % 2 == 0))

      val reports = Stream.awakeEvery[IO](config.reportEvery).evalMap(_ => report(http, config))

      logger.info(
        s"Simulating ${config.usersPerSecond} visits/s in '${config.environment}': conversion " +
          s"${config.controlConversion} (off) vs ${config.treatmentConversion} (on)"
      ) >>
        sdk.awaitReady(30.seconds).flatMap(ready =>
          logger.warn("Flag stream not ready, using defaults").unlessA(ready)
        ) >>
        Stream(traffic, killSwitch, reports).parJoinUnbounded.compile.drain

  private def report(http: Client[IO], config: SimulatorConfig): IO[Unit] =
    val uri =
      (config.statsUri / "stats" / "v1" / "environments" / config.environment.value / "flags" /
        "new-checkout" /
        "metrics" / "purchase").withQueryParam("weights", "on:50,off:50")
    http
      .expect[Json](uri)
      .flatMap: report =>
        val c          = report.hcursor
        val variations = c.downField("variations").focus.flatMap(_.asArray).getOrElse(Vector.empty)
        val summary    = variations
          .map: v =>
            val vc         = v.hcursor
            val users      = vc.get[Long]("users").getOrElse(0L)
            val conversion = vc.get[Long]("conversions").getOrElse(0L).toDouble / math.max(1L, users)
            f"${vc.get[String]("variation").getOrElse("?")}: $users users, ${conversion * 100}%.1f%%"
          .mkString(", ")
        val comparison = c.downField("comparisons").downN(0).downField("conversion")
        val lift       = comparison.get[Option[Double]]("relativeLift").toOption.flatten.getOrElse(0.0)
        val p          = comparison.get[Double]("pValue").getOrElse(1.0)
        val srm        = c.downField("srm").get[Boolean]("detected").getOrElse(false)
        logger.info(f"[experiment new-checkout] $summary | lift ${lift * 100}%+.1f%%, p = $p%.2g, SRM: $srm")
      .handleErrorWith(e => logger.info(s"[experiment new-checkout] no report yet (${e.getMessage})"))

/** The few admin API calls the simulator needs. Every step is idempotent, so restarts are safe. */
final class Admin(http: Client[IO], base: Uri, logger: Logger[IO]):

  private def call(method: Method, uri: Uri, body: Json): IO[Response[IO]] =
    http
      .run(Request[IO](method, uri).withEntity(body).putHeaders(Header.Raw(ci"X-Actor", "simulator")))
      .use(r => IO.pure(r))

  /** Retries until the admin API is reachable (it may still be starting). */
  private def retrying[A](what: String)(io: IO[A]): IO[A] =
    io.handleErrorWith(e =>
      logger.info(s"$what: ${e.getMessage}, retrying in 2s") >> IO.sleep(2.seconds) >> retrying(what)(io)
    )

  def setUp(env: EnvironmentKey): IO[Unit] =
    val api      = base / "api" / "v1"
    val boolFlag = (key: String, name: String) =>
      Json.obj(
        "key"        -> key.asJson,
        "name"       -> name.asJson,
        "kind"       -> "boolean".asJson,
        "variations" -> Json.arr(
          Json.obj("key" -> "on".asJson, "value"  -> Json.True),
          Json.obj("key" -> "off".asJson, "value" -> Json.False)
        )
      )
    def created(what: String)(r: Response[IO]) =
      if r.status.isSuccess || r.status == Status.Conflict then IO.unit
      else IO.raiseError(RuntimeException(s"$what: ${r.status}"))

    retrying("Setting up the demo environment"):
      for
        _ <- call(Method.POST, api / "environments", Json.obj("key" -> env.asJson, "name" -> "Demo".asJson))
          .flatMap(created("environment"))
        _ <- call(
          Method.POST,
          api / "flags",
          boolFlag("new-checkout", "New checkout")
        ).flatMap(created("new-checkout"))
        _ <- call(
          Method.POST,
          api / "flags",
          boolFlag("promo-banner", "Promo banner")
        ).flatMap(created("promo-banner"))
        _ <- configureExperiment(api, env)
        _ <- logger.info(s"Environment '$env' is set up: new-checkout runs a 50/50 experiment")
      yield ()

  private def configureExperiment(api: Uri, env: EnvironmentKey): IO[Unit] =
    val flagUri = api / "environments" / env.value / "flags" / "new-checkout"
    http.expect[Json](flagUri).flatMap: current =>
      val version = current.hcursor.get[Long]("version").getOrElse(1L)
      val config  = Json.obj(
        "enabled"      -> Json.True,
        "offVariation" -> "off".asJson,
        "fallthrough"  -> Json.obj(
          "type"       -> "rollout".asJson,
          "variations" -> Json.arr(
            Json.obj("variation" -> "on".asJson, "weight"  -> 50_000.asJson),
            Json.obj("variation" -> "off".asJson, "weight" -> 50_000.asJson)
          )
        )
      )
      val alreadyRunning = current.hcursor.downField("config").get[Boolean]("enabled").contains(true)
      if alreadyRunning then IO.unit
      else
        call(Method.PUT, flagUri, Json.obj("version" -> version.asJson, "config" -> config)).flatMap: r =>
          IO.raiseError(
            RuntimeException(s"configuring the experiment: ${r.status}")
          ).unlessA(r.status.isSuccess)

  def toggle(env: EnvironmentKey, flag: FlagKey, enabled: Boolean): IO[Unit] =
    call(
      Method.POST,
      base / "api" / "v1" / "environments" / env.value / "flags" / flag.value / "toggle",
      Json.obj("enabled" -> enabled.asJson)
    ).flatMap(r => logger.warn(s"Kill switch '$flag' failed: ${r.status}").unlessA(r.status.isSuccess))
      .handleErrorWith(e => logger.warn(s"Kill switch '$flag' failed: ${e.getMessage}"))
