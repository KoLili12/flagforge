package flagforge.delivery

import cats.effect.{IO, Ref}
import flagforge.delivery.Fixtures.*
import flagforge.domain.*
import fs2.concurrent.SignallingRef
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.ServerSentEvent.EventId
import org.http4s.headers.{`If-None-Match`, `Last-Event-Id`, ETag}
import org.http4s.implicits.*

import scala.concurrent.duration.*

class SdkRoutesSuite extends CatsEffectSuite:

  private final case class Fixture(
      routes: HttpApp[IO],
      state: SignallingRef[IO, DeliveryState],
      ready: SignallingRef[IO, Boolean]
  )

  private def fixture(initial: DeliveryState, isReady: Boolean = true): IO[Fixture] =
    for
      state       <- SignallingRef.of[IO, DeliveryState](initial)
      ready       <- SignallingRef.of[IO, Boolean](isReady)
      connections <- Ref.of[IO, Int](0)
    yield Fixture(SdkRoutes(state, ready, connections, heartbeat = 50.millis).routes.orNotFound, state, ready)

  private val streamUri = uri"/sdk/v1/environments/prod/stream"

  /** First `n` non-heartbeat events of the stream. */
  private def events(app: HttpApp[IO], request: Request[IO], n: Int): IO[List[ServerSentEvent]] =
    app
      .run(request)
      .flatMap(_.body.through(ServerSentEvent.decoder).filter(_.eventType.isDefined).take(n.toLong).compile.toList)
      .timeout(5.seconds)

  private val twoFlags = feed(DeliveryState.empty(), 0, updated(prod, "a", 1), updated(prod, "b", 1))

  test("stream is unavailable until the feed is loaded"):
    fixture(DeliveryState.empty(), isReady = false).flatMap: f =>
      f.routes.run(Request[IO](Method.GET, streamUri)).map(r => assertEquals(r.status, Status.ServiceUnavailable))

  test("a new client gets a put with the full ruleset, then patches"):
    fixture(twoFlags).flatMap: f =>
      val change = f.state.update(_.applied(2, updated(prod, "a", 2, enabled = false))).delayBy(200.millis)
      change.background.surround(events(f.routes, Request[IO](Method.GET, streamUri), 2)).map:
        case put :: patch :: Nil =>
          assertEquals(put.eventType, Some("put"))
          assertEquals(put.id, Some(EventId("1")))
          assertEquals(decode[Ruleset](put.data.get).map(_.flags.size), Right(2))
          assertEquals(patch.eventType, Some("patch"))
          assertEquals(patch.id, Some(EventId("2")))
          assertEquals(decode[ChangeEvent](patch.data.get), Right(updated(prod, "a", 2, enabled = false)))
        case other => fail(s"unexpected events: $other")

  test("a reconnecting client with Last-Event-ID only gets what it missed"):
    val state = feed(twoFlags, 2, updated(dev, "x", 1), updated(prod, "b", 2))
    fixture(state).flatMap: f =>
      val request = Request[IO](Method.GET, streamUri).putHeaders(`Last-Event-Id`(EventId("1")))
      events(f.routes, request, 1).map: received =>
        assertEquals(received.map(e => (e.eventType, e.id)), List((Some("patch"), Some(EventId("3")))))

  test("heartbeats keep the connection alive"):
    fixture(twoFlags).flatMap: f =>
      f.routes
        .run(Request[IO](Method.GET, streamUri))
        .flatMap(_.body.through(ServerSentEvent.decoder).filter(_.comment.isDefined).take(2).compile.toList)
        .timeout(5.seconds)
        .map(hb => assertEquals(hb.flatMap(_.comment), List("heartbeat", "heartbeat")))

  test("ruleset snapshot supports conditional requests"):
    fixture(twoFlags).flatMap: f =>
      val uri = uri"/sdk/v1/environments/prod/ruleset"
      for
        first  <- f.routes.run(Request[IO](Method.GET, uri))
        etag    = first.headers.get[ETag].map(_.tag).get
        second <- f.routes.run(Request[IO](Method.GET, uri).putHeaders(`If-None-Match`(Some(cats.data.NonEmptyList.one(etag)))))
      yield
        assertEquals(first.status, Status.Ok)
        assertEquals(second.status, Status.NotModified)

  test("server-side evaluation"):
    fixture(twoFlags).flatMap: f =>
      val request = Request[IO](Method.POST, uri"/sdk/v1/environments/prod/evaluate")
        .withEntity(EvaluationContext("user-1").asJson.noSpaces)
      f.routes.run(request).flatMap(_.as[String]).map: body =>
        val values = decode[List[Evaluation]](body).map(_.map(_.value))
        assertEquals(values, Right(List(Some(Json.True), Some(Json.True))))

class OutgoingSuite extends munit.FunSuite:

  private val state = feed(DeliveryState.empty(), 0, updated(prod, "a", 1), updated(dev, "a", 1))

  test("first contact is a full sync"):
    val (cursor, messages) = Outgoing.plan(prod, None, state)
    assertEquals(cursor, Some(1L))
    assert(messages.head.isInstanceOf[Outgoing.Put])

  test("an up-to-date client gets nothing"):
    assertEquals(Outgoing.plan(prod, Some(1), state), (Some(1L), Vector.empty))

  test("changes of other environments advance the cursor silently"):
    assertEquals(Outgoing.plan(prod, Some(0), state), (Some(1L), Vector.empty))
