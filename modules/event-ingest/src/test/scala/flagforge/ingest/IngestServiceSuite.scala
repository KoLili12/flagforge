package flagforge.ingest

import cats.effect.{IO, Ref}
import flagforge.domain.*
import munit.CatsEffectSuite

import scala.concurrent.duration.*

object Events:
  val prod: EnvironmentKey = EnvironmentKey.applyUnsafe("prod")
  val now: Long            = System.currentTimeMillis()

  def exposure(
      user: String,
      flag: String = "checkout",
      variation: String = "on",
      env: EnvironmentKey = prod
  ) =
    Exposure(env, FlagKey.applyUnsafe(flag), VariationKey.applyUnsafe(variation), user, 1, now)

  def purchase(id: String, user: String, amount: Double = 10) =
    MetricEvent(id, prod, MetricKey.applyUnsafe("purchase"), user, Some(amount), now)

class IngestServiceSuite extends CatsEffectSuite:
  import Events.*

  private final case class Fixture(
      service: IngestService[IO],
      published: Ref[IO, List[EventBatch]],
      failing: Ref[IO, Boolean]
  )

  private def fixture(limit: RateLimit = RateLimit(1000, 100)): IO[Fixture] =
    for
      guards    <- Guards.inMemory[IO](limit)
      published <- Ref.of[IO, List[EventBatch]](Nil)
      failing   <- Ref.of[IO, Boolean](false)
      publisher = new EventPublisher[IO]:
        def publish(exposures: List[Exposure], metrics: List[MetricEvent]): IO[Unit] =
          failing.get.ifM(
            IO.raiseError(RuntimeException("kafka is down")),
            published.update(_ :+ EventBatch(exposures, metrics))
          )
    yield Fixture(IngestService(guards, publisher, IngestLimits(maxBatch = 100)), published, failing)

  test("new events are published, duplicates within and across batches are dropped"):
    fixture().flatMap: f =>
      val batch = EventBatch(
        List(exposure("u1"), exposure("u1", variation = "off"), exposure("u2")),
        List(purchase("p1", "u1"), purchase("p1", "u1"))
      )
      for
        first  <- f.service.ingest(prod, "client", batch)
        second <- f.service.ingest(prod, "client", batch)
        sent   <- f.published.get
      yield
        assertEquals(first, IngestResult.Accepted(accepted = 3, duplicates = 2))
        assertEquals(second, IngestResult.Accepted(accepted = 0, duplicates = 5))
        assertEquals(
          sent.flatMap(_.exposures).map(e => (e.contextKey, e.variation.value: String)),
          List(("u1", "on"), ("u2", "on"))
        )

  test("invalid batches are rejected with every problem listed"):
    fixture().flatMap: f =>
      val future = exposure("u1").copy(timestamp = now + 1.hour.toMillis)
      val batch  = EventBatch(List(future, exposure("", env = EnvironmentKey.applyUnsafe("dev"))))
      f.service.ingest(prod, "client", batch).map:
        case IngestResult.Invalid(errors) => assertEquals(errors.size, 3, errors)
        case other                        => fail(s"expected Invalid, got $other")

  test("clients over their rate limit are told when to come back"):
    fixture(RateLimit(capacity = 3, refillPerSecond = 1)).flatMap: f =>
      val batch = EventBatch(List(exposure("a"), exposure("b")))
      for
        first  <- f.service.ingest(prod, "greedy", batch)
        second <- f.service.ingest(prod, "greedy", batch)
        other  <- f.service.ingest(prod, "polite", batch)
      yield
        assert(first.isInstanceOf[IngestResult.Accepted])
        assert(second.isInstanceOf[IngestResult.RateLimited], second)
        assert(other.isInstanceOf[IngestResult.Accepted], "limits are per client")

  test("if publishing fails, a retry of the same batch is not treated as a duplicate"):
    fixture().flatMap: f =>
      val batch = EventBatch(List(exposure("u1")), List(purchase("p1", "u1")))
      for
        _     <- f.failing.set(true)
        error <- f.service.ingest(prod, "client", batch).attempt
        _     <- f.failing.set(false)
        retry <- f.service.ingest(prod, "client", batch)
      yield
        assert(error.isLeft)
        assertEquals(retry, IngestResult.Accepted(accepted = 2, duplicates = 0))
