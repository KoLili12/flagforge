package flagforge.it

import cats.effect.IO
import cats.syntax.all.*
import flagforge.domain.*
import flagforge.it.Pipeline.given
import flagforge.sdk.{EventsConfig, Flag, FlagforgeClient, SdkConfig}
import flagforge.stats.ExperimentReport
import fs2.Stream
import io.circe.Json
import io.circe.literal.*
import io.circe.parser.decode
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.`Content-Type`

import scala.concurrent.duration.*
import scala.util.Random

/** An A/B test from start to finish, with a known true effect:
  *
  * admin API sets up a 50/50 rollout → SDK assigns 2000 simulated users and records exposures → users in `on`
  * convert at 30%, in `off` at 20% → events flow through ingest and Kafka into stats-engine → the report must
  * find the effect and must not raise a sample ratio mismatch.
  */
class ExperimentSuite extends CatsEffectSuite:

  override val munitIOTimeout: Duration = 4.minutes

  private val pipeline       = ResourceSuiteLocalFixture("pipeline", Pipeline.resource)
  override def munitFixtures = List(pipeline)

  private val CheckoutV2 = Flag.bool("checkout-v2", default = false)
  private val Purchase   = MetricKey("purchase")
  private val Users      = 2000

  private def setUpExperiment(p: Pipeline): IO[Unit] =
    val calls = List(
      (Method.POST, "/api/v1/environments", json"""{"key": "e2e-exp", "name": "Experiments"}"""),
      (
        Method.POST,
        "/api/v1/flags",
        json"""{"key": "checkout-v2", "name": "Checkout v2", "kind": "boolean",
                "variations": [{"key": "on", "value": true}, {"key": "off", "value": false}]}"""
      ),
      (
        Method.PUT,
        "/api/v1/environments/e2e-exp/flags/checkout-v2",
        json"""{"version": 1, "config": {"enabled": true, "offVariation": "off",
                "fallthrough": {"type": "rollout", "variations": [
                  {"variation": "on", "weight": 50000}, {"variation": "off", "weight": 50000}]}}}"""
      )
    )
    calls.traverse_ { (method, path, body) =>
      p.call(method, path, Some(body)).map((status, json) =>
        assert(status.isSuccess, s"$path: $status $json")
      )
    }

  private def report(p: Pipeline): IO[Option[ExperimentReport]] =
    val uri = Uri.unsafeFromString(
      "/stats/v1/environments/e2e-exp/flags/checkout-v2/metrics/purchase?weights=on:50,off:50"
    )
    p.stats.run(Request[IO](Method.GET, uri)).flatMap(_.as[String]).map(decode[ExperimentReport](_).toOption)

  test("a simulated A/B test is detected by the stats engine"):
    val p      = pipeline()
    val random = Random(2026)
    val config = SdkConfig(
      p.deliveryUri,
      EnvironmentKey.applyUnsafe("e2e-exp"),
      events = EventsConfig(Some(p.ingestUri), flushInterval = 200.millis)
    )
    for
      _           <- setUpExperiment(p)
      assignments <- FlagforgeClient
        .resource[IO](config)
        .use: client =>
          val rolledOut = client.ruleset.discrete
            .find(_.exists(_.flags.get(CheckoutV2.key).exists(_.config.enabled)))
            .compile
            .drain
            .timeout(30.seconds)
          rolledOut >> (1 to Users).toList.traverse: i =>
            val user = EvaluationContext(s"user-$i")
            client.get(CheckoutV2, user).flatTap: enabled =>
              val converts = random.nextDouble() < (if enabled then 0.30 else 0.20)
              client.track(Purchase, user, Some(10 + random.nextInt(90).toDouble)).whenA(converts)
      // The client is closed here: its remaining events are flushed.
      onShare = assignments.count(identity).toDouble / Users
      result <- Stream
        .repeatEval(report(p))
        .metered(500.millis)
        .collectFirst { case Some(r) if r.variations.map(_.users).sum == Users => r }
        .compile
        .lastOrError
        .timeout(90.seconds)
      _ <- IO.println(
        f"[e2e] ${result.variations.map(v =>
            f"${v.variation}: ${v.users} users, ${v.conversionRate * 100}%.1f%% conv"
          ).mkString(", ")}; " +
          f"p = ${result.comparisons.head.conversion.pValue}%.2e, SRM p = ${result.srm.pValue}%.2f"
      )
    yield
      assert(onShare > 0.45 && onShare < 0.55, s"rollout share $onShare")
      assertEquals(result.control, "off")
      val on = result.comparisons.head
      assertEquals(on.variation, "on")
      assert(on.significant, on)
      assert(on.conversion.ciLow > 0, s"CI must exclude zero: $on")
      assert(on.conversion.difference > 0.05 && on.conversion.difference < 0.15, on.conversion)
      assert(!result.srm.detected, result.srm)

  test("ingest drops repeated exposures and metric events"):
    val p     = pipeline()
    val batch = EventBatch(
      List(Exposure(
        EnvironmentKey.applyUnsafe("e2e-dedup"),
        CheckoutV2.key,
        VariationKey.applyUnsafe("on"),
        "u1",
        1,
        1
      )),
      List(MetricEvent("evt-1", EnvironmentKey.applyUnsafe("e2e-dedup"), Purchase, "u1", Some(5), 2))
    )
    EmberClientBuilder.default[IO].build.use: http =>
      val send = http
        .run(
          Request[IO](Method.POST, p.ingestUri / "events" / "v1" / "environments" / "e2e-dedup" / "batch")
            .withEntity(batch.asJson.noSpaces)
            .withContentType(`Content-Type`(MediaType.application.json))
        )
        .use(r => r.as[String].map(body => (r.status, io.circe.parser.parse(body).getOrElse(Json.Null))))
      (send, send).tupled.map:
        case ((s1, first), (s2, second)) =>
          assertEquals((s1, s2), (Status.Accepted, Status.Accepted))
          assertEquals(first, json"""{"accepted": 2, "duplicates": 0}""")
          assertEquals(second, json"""{"accepted": 0, "duplicates": 2}""")
