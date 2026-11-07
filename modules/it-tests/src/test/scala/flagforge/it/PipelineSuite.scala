package flagforge.it

import cats.effect.IO
import flagforge.delivery.DeliveryApp
import flagforge.domain.*
import flagforge.it.Pipeline.given
import flagforge.sdk.{Flag, FlagforgeClient, SdkConfig}
import fs2.Stream
import io.circe.Json
import io.circe.literal.*
import munit.CatsEffectSuite
import org.http4s.*
import skunk.codec.all.int8
import skunk.implicits.*

import scala.concurrent.duration.*

class PipelineSuite extends CatsEffectSuite:

  override val munitIOTimeout: Duration = 3.minutes

  private val pipeline       = ResourceSuiteLocalFixture("pipeline", Pipeline.resource)
  override def munitFixtures = List(pipeline)

  private val NewCheckout = Flag.bool("new-checkout", default = false)

  private def setUp(p: Pipeline, env: String): IO[Unit] =
    for
      (envStatus, _) <-
        p.call(Method.POST, "/api/v1/environments", Some(json"""{"key": $env, "name": $env}"""))
      (flagStatus, _) <- p.call(
        Method.POST,
        "/api/v1/flags",
        Some(json"""{
          "key": "new-checkout", "name": "New checkout", "kind": "boolean",
          "variations": [{"key": "on", "value": true}, {"key": "off", "value": false}]
        }""")
      )
    yield
      assertEquals(envStatus, Status.Created)
      assert(
        flagStatus == Status.Created || flagStatus == Status.Conflict,
        flagStatus
      ) // flag is shared by tests

  private def toggle(p: Pipeline, env: String, enabled: Boolean): IO[Unit] =
    p.call(
      Method.POST,
      s"/api/v1/environments/$env/flags/new-checkout/toggle",
      Some(json"""{"enabled": $enabled}""")
    )
      .map((status, body) => assertEquals(status, Status.Ok, body))

  test("a kill switch flipped in the admin API reaches a connected SDK in real time"):
    val p    = pipeline()
    val user = EvaluationContext("user-1")
    setUp(p, "e2e-live") >>
      FlagforgeClient
        .resource[IO](SdkConfig(p.deliveryUri, EnvironmentKey.applyUnsafe("e2e-live")))
        .use: client =>
          for
            ready   <- client.awaitReady(30.seconds)
            initial <- client.get(NewCheckout, user)
            start   <- IO.monotonic
            _       <- toggle(p, "e2e-live", enabled = true)
            _       <- client.watch(NewCheckout, user).find(identity).compile.lastOrError.timeout(15.seconds)
            latency <- IO.monotonic.map(_ - start)
            _       <- IO.println(s"[e2e] admin API → Postgres → Kafka → SSE → SDK: ${latency.toMillis} ms")
            off     <- toggle(p, "e2e-live", enabled = false) >>
              client.watch(NewCheckout, user).find(!_).compile.lastOrError.timeout(15.seconds)
          yield
            assert(ready)
            assertEquals(initial, false)
            assert(latency < 5.seconds, s"propagation took $latency")
            assertEquals(off, false)

  test("every outbox row is eventually published"):
    val p           = pipeline()
    val unpublished =
      p.adminDb.use(_.unique(sql"SELECT count(*) FROM outbox WHERE published_at IS NULL".query(int8)))
    Stream
      .repeatEval(unpublished)
      .metered(200.millis)
      .find(_ == 0L)
      .compile
      .lastOrError
      .timeout(15.seconds)
      .assertEquals(0L)

  test("a new delivery instance warm-starts from the Redis snapshot"):
    val p   = pipeline()
    val env = EnvironmentKey.applyUnsafe("e2e-warm")
    p.snapshots.use: snapshots =>
      val snapshotWithFlag = Stream
        .repeatEval(snapshots.load)
        .metered(200.millis)
        .collectFirst { case Some(s) if s.environments.get(env).exists(_.flags.nonEmpty) => s }
        .compile
        .lastOrError
        .timeout(30.seconds)
      for
        _        <- setUp(p, "e2e-warm")
        snapshot <- snapshotWithFlag
        _        <- DeliveryApp
          .resource[IO](p.deliveryConfig, snapshots, flagforge.telemetry.Telemetry.noop[IO])
          .use: second =>
            for
              restored <- second.state.get // before the consumer had any chance to catch up
              ready    <- second.ready.discrete.find(identity).compile.lastOrError.timeout(30.seconds)
              current  <- second.state.get
            yield
              assert(restored.offset >= snapshot.offset, s"${restored.offset} < ${snapshot.offset}")
              assert(
                restored.ruleset(env).flags.nonEmpty,
                "the flag must come from the snapshot, not from Kafka"
              )
              assert(ready)
              assertEquals(current.ruleset(env).flags.keySet, restored.ruleset(env).flags.keySet)
      yield ()
