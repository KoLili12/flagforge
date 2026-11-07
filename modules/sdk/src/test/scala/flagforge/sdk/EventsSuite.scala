package flagforge.sdk

import cats.effect.{IO, Ref}
import com.comcast.ip4s.*
import flagforge.domain.*
import fs2.Stream
import io.circe.parser.decode
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.ServerSentEvent.EventId
import org.http4s.dsl.io.*
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.noop.NoOpFactory

import scala.concurrent.duration.*

class EventsSuite extends CatsEffectSuite:
  import TestData.*

  given LoggerFactory[IO] = NoOpFactory[IO]

  private val user     = EvaluationContext("user-1")
  private val purchase = MetricKey("purchase")

  /** A server that streams one ruleset and records the analytics batches it receives. */
  private def withServer[A](failFirst: Int)(test: (SdkConfig, IO[List[EventBatch]]) => IO[A]): IO[A] =
    for
      received <- Ref.of[IO, List[EventBatch]](Nil)
      failures <- Ref.of[IO, Int](failFirst)
      app = HttpRoutes
        .of[IO] {
          case GET -> Root / "sdk" / "v1" / "environments" / "prod" / "stream" =>
            val put = ServerSentEvent(Some(ruleset(checkout(1, enabled = true)).asJson.noSpaces), Some("put"), Some(EventId("0")))
            Ok(Stream.emit(put).covary[IO] ++ Stream.never[IO])
          case req @ POST -> Root / "events" / "v1" / "environments" / "prod" / "batch" =>
            failures.getAndUpdate(n => (n - 1).max(0)).flatMap:
              case n if n > 0 => ServiceUnavailable()
              case _ =>
                req.as[String].flatMap(body => received.update(_ :+ decode[EventBatch](body).toOption.get)) >> Accepted()
        }
        .orNotFound
      result <- EmberServerBuilder
        .default[IO]
        .withHost(host"127.0.0.1")
        .withPort(port"0")
        .withHttpApp(app)
        .withShutdownTimeout(1.second)
        .build
        .use: server =>
          val config = SdkConfig(
            server.baseUri,
            prod,
            events = EventsConfig(Some(server.baseUri), flushInterval = 1.minute, maxAttempts = 3)
          )
          test(config, received.get)
    yield result

  test("exposures are recorded once per flag, variation and context; metrics are tracked; close flushes"):
    withServer(failFirst = 0): (config, received) =>
      FlagforgeClient
        .resource[IO](config)
        .use: client =>
          client.awaitReady(5.seconds).assert >>
            client.get(NewCheckout, user).replicateA_(10) >>
            client.get(NewCheckout, EvaluationContext("user-2")) >>
            client.track(purchase, user, Some(42.5)) >>
            received.map(sent => assertEquals(sent, Nil, "nothing is sent before the flush interval"))
        .flatMap(_ => received)
        .map: sent =>
          val exposures = sent.flatMap(_.exposures)
          assertEquals(exposures.map(e => (e.contextKey, e.variation.value: String, e.flagVersion)), List(("user-1", "on", 1L), ("user-2", "on", 1L)))
          assertEquals(sent.flatMap(_.metrics).map(m => (m.metric.value: String, m.value)), List(("purchase", Some(42.5))))

  test("defaults are not exposures"):
    withServer(failFirst = 0): (config, received) =>
      FlagforgeClient
        .resource[IO](config)
        .use(client => client.awaitReady(5.seconds) >> client.get(Flag.bool("unknown-flag", false), user))
        .flatMap(_ => received)
        .map(sent => assertEquals(sent.flatMap(_.exposures), Nil))

  test("failed deliveries are retried"):
    withServer(failFirst = 2): (config, received) =>
      FlagforgeClient
        .resource[IO](config)
        .use(client => client.awaitReady(5.seconds) >> client.track(purchase, user))
        .flatMap(_ => received)
        .map(sent => assertEquals(sent.flatMap(_.metrics).size, 1))
