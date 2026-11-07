package flagforge.ingest

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.dimafeng.testcontainers.GenericContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import munit.CatsEffectSuite
import org.testcontainers.containers.wait.strategy.Wait
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.noop.NoOpFactory

import scala.concurrent.duration.*

/** The Lua scripts against a real Redis. */
class RedisGuardsSuite extends CatsEffectSuite with TestContainerForAll:

  given LoggerFactory[IO] = NoOpFactory[IO]

  override val containerDef: GenericContainer.Def[GenericContainer] =
    GenericContainer.Def("redis:7.4-alpine", exposedPorts = Seq(6379), waitStrategy = Wait.forListeningPort())

  private def guards(limit: RateLimit): Resource[IO, Guards[IO]] =
    Resource
      .eval(IO(withContainers(c => s"redis://${c.host}:${c.mappedPort(6379)}")))
      .flatMap(Guards.redis[IO](_, limit, dedupTtl = 1.hour))

  test("token bucket: allows the capacity, then limits with a retry hint, then refills"):
    guards(RateLimit(capacity = 10, refillPerSecond = 20)).use: g =>
      for
        burst    <- (1 to 5).toList.traverse(_ => g.acquire("bucket-a", 2))
        limited  <- g.acquire("bucket-a", 4)
        _        <- IO.sleep(250.millis)
        refilled <- g.acquire("bucket-a", 4)
      yield
        assertEquals(burst, List.fill(5)(RateDecision.Allowed))
        limited match
          case RateDecision.Limited(retry) => assert(retry > 0.millis && retry <= 200.millis, retry)
          case other                       => fail(s"expected Limited, got $other")
        assertEquals(refilled, RateDecision.Allowed)

  test("concurrent requests never overdraw the bucket"):
    guards(RateLimit(capacity = 50, refillPerSecond = 1)).use: g =>
      (1 to 100).toList.parTraverse(_ => g.acquire("bucket-b", 1)).map: decisions =>
        assertEquals(decisions.count(_ == RateDecision.Allowed), 50)

  test("dedup marks keys once and forget releases them"):
    guards(RateLimit(10, 10)).use: g =>
      for
        first  <- g.firstSeen(List("k1", "k2"))
        second <- g.firstSeen(List("k2", "k3"))
        _      <- g.forget(List("k1"))
        third  <- g.firstSeen(List("k1"))
        empty  <- g.firstSeen(Nil)
      yield
        assertEquals(first, List(true, true))
        assertEquals(second, List(false, true))
        assertEquals(third, List(true))
        assertEquals(empty, Nil)
