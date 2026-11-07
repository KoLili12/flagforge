package flagforge.sdk

import cats.effect.{IO, Ref}
import flagforge.domain.*
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.ServerSentEvent.EventId
import com.comcast.ip4s.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.dsl.io.*
import org.http4s.headers.`Last-Event-Id`
import org.http4s.implicits.*
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.noop.NoOpFactory

import scala.concurrent.duration.*

object TestData:
  val prod: EnvironmentKey = EnvironmentKey.applyUnsafe("prod")
  val on: VariationKey     = VariationKey.applyUnsafe("on")
  val off: VariationKey    = VariationKey.applyUnsafe("off")
  val NewCheckout          = Flag.bool("new-checkout", default = false)

  def checkout(version: Long, enabled: Boolean): FeatureFlag =
    FeatureFlag(
      NewCheckout.key,
      FlagKind.Boolean,
      List(Variation(on, Json.True), Variation(off, Json.False)),
      "salt",
      version,
      FlagConfig(enabled, off, Serve.Fixed(on))
    )

  def ruleset(flags: FeatureFlag*): Ruleset = Ruleset(prod, flags.map(f => f.key -> f).toMap)

class FlagSuite extends munit.FunSuite:
  import TestData.*

  test("flag keys are validated at compile time"):
    assert(compileErrors("""Flag.bool("new-checkout", false)""").isEmpty)
    assert(compileErrors("""Flag.bool("Not A Valid Key", false)""").nonEmpty)

  test("dynamic keys are validated at runtime"):
    assert(Flag.dynamic("Bad Key", 0)(_.asNumber.flatMap(_.toInt)).isLeft)

  test("defaults are served before the first ruleset arrives"):
    val result = LocalEvaluation.evaluate(None, NewCheckout, EvaluationContext("u"))
    assertEquals(result.value, false)
    assert(result.isDefault)

  test("a value of the wrong type falls back to the default"):
    val asString = Flag.string("new-checkout", default = "fallback")
    val result   = LocalEvaluation.evaluate(Some(ruleset(checkout(1, enabled = true))), asString, EvaluationContext("u"))
    assertEquals(result.value, "fallback")
    assertEquals(result.reason.asInstanceOf[Reason.Error].kind, ErrorKind.WrongType)

  test("served values are decoded"):
    val result = LocalEvaluation.evaluate(Some(ruleset(checkout(1, enabled = true))), NewCheckout, EvaluationContext("u"))
    assertEquals((result.value, result.variation), (true, Some(on)))

class ClientSuite extends CatsEffectSuite:
  import TestData.*

  given LoggerFactory[IO] = NoOpFactory[IO]

  private def sse(event: String, id: Long, data: Json) =
    ServerSentEvent(Some(data.noSpaces), Some(event), Some(EventId(id.toString)))

  test("applies put and patch, then resumes with Last-Event-ID after the server drops the stream"):
    for
      requests <- Ref.of[IO, List[Option[String]]](Nil)
      app = HttpRoutes
        .of[IO] { case req @ GET -> Root / "sdk" / "v1" / "environments" / "prod" / "stream" =>
          requests.getAndUpdate(_ :+ req.headers.get[`Last-Event-Id`].map(_.id.value)).flatMap {
            // First connection: full state, one change, then the server goes away.
            case Nil =>
              Ok(
                Stream(
                  sse("put", 5, ruleset(checkout(1, enabled = true)).asJson),
                  sse("patch", 6, (ChangeEvent.FlagUpdated(prod, checkout(2, enabled = false)): ChangeEvent).asJson)
                ).covary[IO]
              )
            // Reconnection: a newer change, then the stream stays open.
            case _ =>
              Ok(
                Stream.emit(sse("patch", 7, (ChangeEvent.FlagUpdated(prod, checkout(3, enabled = true)): ChangeEvent).asJson))
                  .covary[IO] ++ Stream.never[IO]
              )
          }
        }
        .orNotFound
      server = EmberServerBuilder.default[IO].withHost(host"127.0.0.1").withPort(port"0").withHttpApp(app).withShutdownTimeout(1.second).build
      values <- server
        .flatMap: srv =>
          val config = SdkConfig(srv.baseUri, prod, minBackoff = 10.millis, maxBackoff = 50.millis)
          FlagforgeClient.resource[IO](config)
        .use: client =>
          client.awaitReady(5.seconds).assert >>
            client.watch(NewCheckout, EvaluationContext("u")).dropWhile(_ == false).take(1).compile.toList.timeout(5.seconds)
      seen <- requests.get
    yield
      assertEquals(values, List(true))
      assertEquals(seen.take(2), List(None, Some("6")))
