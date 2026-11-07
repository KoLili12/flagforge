package flagforge.loadtest

import cats.effect.std.{AtomicCell, Semaphore}
import cats.effect.{Deferred, IO, IOApp, Ref}
import cats.syntax.all.*
import flagforge.domain.*
import fs2.Stream
import io.circe.Json
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.CirceEntityCodec.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.headers.Connection
import org.typelevel.ci.*

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Load tests against a running system (e.g. `docker compose --profile apps up`).
  *
  * {{{
  * sbt "loadTest/run sse connections=2000 rounds=10"
  * sbt "loadTest/run ingest clients=50 batch=100 seconds=30"
  * }}}
  *
  * Options: admin=, delivery=, ingest= (base URLs, default localhost:8080/8081/8083).
  */
object Main extends IOApp:

  final case class Options(values: Map[String, String]):
    def int(key: String, default: Int): Int    = values.get(key).flatMap(_.toIntOption).getOrElse(default)
    def uri(key: String, default: String): Uri = Uri.unsafeFromString(values.getOrElse(key, default))

  def run(args: List[String]): IO[cats.effect.ExitCode] =
    val options = Options(args.drop(1).flatMap(a =>
      a.split("=", 2) match
        case Array(k, v) => Some(k -> v)
        case _           => None
    ).toMap)
    val connections = options.int("connections", 1000)
    EmberClientBuilder
      .default[IO]
      .withMaxTotal(connections + 256)
      .withMaxPerKey(_ => connections + 256)
      .withIdleConnectionTime(2.minutes)
      .withTimeout(60.seconds)
      .build
      .use: http =>
        args.headOption match
          case Some("sse")    => SseFanOut(http, options).run.as(cats.effect.ExitCode.Success)
          case Some("ingest") => IngestThroughput(http, options).run.as(cats.effect.ExitCode.Success)
          case _              =>
            IO.println("usage: loadTest/run (sse|ingest) [key=value ...]").as(cats.effect.ExitCode.Error)

object Stats:
  def percentile(sorted: Vector[Double], p: Double): Double =
    if sorted.isEmpty then Double.NaN
    else sorted(math.min(sorted.size - 1, math.ceil(p / 100 * sorted.size).toInt - 1).max(0))

  def summary(values: Iterable[Double], unit: String = "ms"): String =
    val s = values.toVector.sorted
    f"p50 ${percentile(s, 50)}%.1f$unit  p95 ${percentile(s, 95)}%.1f$unit  p99 ${percentile(s, 99)}%.1f$unit  max ${s.lastOption.getOrElse(Double.NaN)}%.1f$unit"

/** Opens N SSE connections, flips a flag R times and measures, for every connection, the time from the admin
  * API call to the `patch` event arriving: the real end-to-end fan-out latency (admin API → Postgres → outbox
  * relay → Kafka → flag-delivery → N sockets).
  */
final class SseFanOut(http: Client[IO], options: Main.Options):
  private val connections = options.int("connections", 1000)
  private val rounds      = options.int("rounds", 10)
  private val admin       = options.uri("admin", "http://localhost:8080")
  private val delivery    = options.uri("delivery", "http://localhost:8081")
  private val env         = "loadtest"
  private val flag        = "fanout-probe"

  def run: IO[Unit] =
    for
      _         <- Admin(http, admin).ensureFlag(env, flag)
      connected <- Ref.of[IO, Int](0)
      arrivals = ConcurrentLinkedQueue[(Int, Long)]()
      round   <- AtomicCell[IO].of(-1)
      gate    <- Semaphore[IO](200) // open connections in waves, not all at once
      started <- IO.monotonic
      _       <- IO.println(s"Opening $connections SSE connections to $delivery ...")
      // A permit is held only until the connection got its initial put, so at most 200 are being set up at once.
      _ <- (1 to connections).toList.parTraverse_ { i =>
        gate.permit.use: _ =>
          Deferred[IO, Unit].flatMap: ready =>
            connect(i, connected, round, arrivals, ready.complete(()).void).start >>
              ready.get.timeout(60.seconds)
      }.start
      _ <- Stream
        .repeatEval(connected.get)
        .metered(1.second)
        .evalTap(n => IO.println(s"  connected: $n").whenA(n < connections))
        .find(_ >= connections)
        .compile
        .drain
        .timeout(5.minutes)
      opened <- IO.monotonic.map(_ - started)
      _      <- IO.println(
        f"All $connections connections received the initial put in ${opened.toMillis / 1000.0}%.1fs"
      )
      perRound <- (0 until rounds).toList.traverse(r => flip(r, round, arrivals))
      all = perRound.flatten
      _ <- IO.println(s"\n=== SSE fan-out: $connections clients, $rounds flag changes ===")
      _ <- IO.println(s"delivered ${all.size} of ${connections * rounds} expected patches")
      _ <- IO.println(s"latency (admin API call → patch at client): ${Stats.summary(all)}")
    yield ()

  private def flip(
      r: Int,
      round: AtomicCell[IO, Int],
      arrivals: ConcurrentLinkedQueue[(Int, Long)]
  ): IO[List[Double]] =
    for
      _     <- IO.delay(arrivals.clear())
      start <- IO.realTime.map(_.toMillis)
      _     <- round.set(r)
      _     <- Admin(http, admin).flip(env, flag)
      _ <- Stream.repeatEval(IO.delay(arrivals.size)).metered(20.millis).find(_ >= connections).compile.drain
        .timeoutTo(30.seconds, IO.unit)
      latencies = arrivals.asScala.toList.collect { case (`r`, at) => (at - start).toDouble }
      _ <- IO.println(f"round ${r + 1}%2d: ${latencies.size}%5d/$connections  ${Stats.summary(latencies)}")
    yield latencies

  private def connect(
      i: Int,
      connected: Ref[IO, Int],
      round: AtomicCell[IO, Int],
      arrivals: ConcurrentLinkedQueue[(Int, Long)],
      onReady: IO[Unit]
  ): IO[Unit] =
    val request = Request[IO](Method.GET, delivery / "sdk" / "v1" / "environments" / env / "stream")
      .putHeaders(Connection.close)
    val ready = Ref.unsafe[IO, Boolean](false)
    http
      .stream(request)
      .flatMap(_.body.through(ServerSentEvent.decoder[IO]))
      .evalMap: event =>
        event.eventType match
          case Some("put") =>
            ready.getAndSet(true).flatMap(seen => (connected.update(_ + 1) >> onReady).unlessA(seen))
          case Some("patch") =>
            (round.get, IO.realTime).flatMapN((r, now) => IO.delay(arrivals.add((r, now.toMillis))).void)
          case _ => IO.unit
      .compile
      .drain
      .handleErrorWith(e => IO.println(s"connection $i failed: ${e.getMessage}"))

/** C concurrent clients post batches of unique exposures for D seconds. */
final class IngestThroughput(http: Client[IO], options: Main.Options):
  private val clients = options.int("clients", 50)
  private val batch   = options.int("batch", 100)
  private val seconds = options.int("seconds", 30)
  private val ingest  = options.uri("ingest", "http://localhost:8083")
  private val env     = EnvironmentKey.applyUnsafe("loadtest")

  def run: IO[Unit] =
    for
      latencies <- IO.delay(ConcurrentLinkedQueue[Double]())
      statuses  <- Ref.of[IO, Map[Int, Long]](Map.empty)
      accepted  <- Ref.of[IO, Long](0)
      _     <- IO.println(s"$clients clients × batches of $batch events for ${seconds}s against $ingest ...")
      start <- IO.monotonic
      deadline = start + seconds.seconds
      _ <- (0 until clients).toList.parTraverse_(c => client(c, deadline, latencies, statuses, accepted))
      elapsed  <- IO.monotonic.map(_ - start)
      byStatus <- statuses.get
      total    <- accepted.get
      _        <- IO.println(s"\n=== Ingest: $clients clients, batch $batch, ${elapsed.toSeconds}s ===")
      _ <- IO.println(f"accepted events: $total (${total / elapsed.toMillis.toDouble * 1000}%.0f events/s)")
      _ <-
        IO.println(s"requests by status: ${byStatus.toList.sorted.map((s, n) => s"$s: $n").mkString(", ")}")
      _ <- IO.println(s"request latency: ${Stats.summary(latencies.asScala)}")
    yield ()

  private def client(
      c: Int,
      deadline: FiniteDuration,
      latencies: ConcurrentLinkedQueue[Double],
      statuses: Ref[IO, Map[Int, Long]],
      accepted: Ref[IO, Long]
  ): IO[Unit] =
    def loop(n: Long): IO[Unit] = IO.monotonic.flatMap: now =>
      if now >= deadline then IO.unit
      else
        val events = (0 until batch).toList.map: i =>
          Exposure(
            env,
            FlagKey.applyUnsafe("ingest-probe"),
            VariationKey.applyUnsafe("on"),
            s"c$c-b$n-u$i",
            1,
            now.toMillis
          )
        val request =
          Request[IO](Method.POST, ingest / "events" / "v1" / "environments" / env.value / "batch")
            .withEntity(EventBatch(events).asJson)
            .putHeaders(Header.Raw(ci"X-Client-Id", s"load-$c"))
        for
          t0     <- IO.monotonic
          result <- http.run(request).use(r => r.as[Json].attempt.map(body => (r.status.code, body.toOption)))
          t1     <- IO.monotonic
          (status, body) = result
          _ <- IO.delay(latencies.add((t1 - t0).toMicros / 1000.0))
          _ <- statuses.update(m => m.updated(status, m.getOrElse(status, 0L) + 1))
          _ <- body.flatMap(_.hcursor.get[Long]("accepted").toOption).traverse_(n => accepted.update(_ + n))
          _ <- loop(n + 1)
        yield ()
    loop(0)

/** Minimal admin API client. */
final class Admin(http: Client[IO], base: Uri):
  private val api = base / "api" / "v1"

  private def send(method: Method, uri: Uri, body: Json): IO[Status] =
    http.run(Request[IO](method, uri).withEntity(body)).use(r => IO.pure(r.status))

  def ensureFlag(env: String, flag: String): IO[Unit] =
    for
      _ <- send(Method.POST, api / "environments", Json.obj("key" -> env.asJson, "name" -> env.asJson))
      _ <- send(
        Method.POST,
        api / "flags",
        Json.obj(
          "key"        -> flag.asJson,
          "name"       -> flag.asJson,
          "kind"       -> "boolean".asJson,
          "variations" -> Json.arr(
            Json.obj("key" -> "on".asJson, "value"  -> Json.True),
            Json.obj("key" -> "off".asJson, "value" -> Json.False)
          )
        )
      )
    yield ()

  /** Turns the flag to the opposite of its current state, so every call produces a change. */
  def flip(env: String, flag: String): IO[Unit] =
    http
      .expect[Json](api / "environments" / env / "flags" / flag)
      .map(_.hcursor.downField("config").get[Boolean]("enabled").getOrElse(false))
      .flatMap(enabled => toggle(env, flag, !enabled))

  def toggle(env: String, flag: String, enabled: Boolean): IO[Unit] =
    send(
      Method.POST,
      api / "environments" / env / "flags" / flag / "toggle",
      Json.obj("enabled" -> enabled.asJson)
    )
      .flatMap(s => IO.raiseError(RuntimeException(s"toggle failed: $s")).unlessA(s.isSuccess))
