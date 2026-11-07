package flagforge.statsengine

import cats.effect.IO
import ciris.Secret
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import flagforge.domain.*
import flagforge.stats.{ExperimentReport, VariationStats}
import io.circe.parser.decode
import munit.CatsEffectSuite
import org.http4s.*
import org.http4s.implicits.*
import org.testcontainers.utility.DockerImageName

class StatsStoreSuite extends CatsEffectSuite with TestContainerForAll:

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(DockerImageName.parse("postgres:17-alpine"))

  private def withStore[A](f: StatsStore[IO] => IO[A]): IO[A] =
    withContainers: pg =>
      val config =
        DbConfig(pg.host, pg.mappedPort(5432), pg.username, Secret(pg.password), pg.databaseName, 4)
      StatsStore.migrate[IO](config) >> StatsStore.pool[IO](config).use(pool => f(StatsStore.postgres(pool)))

  private val env      = EnvironmentKey.applyUnsafe("prod")
  private val purchase = MetricKey.applyUnsafe("purchase")

  private def exposure(flag: String, user: String, variation: String, at: Long) =
    Exposure(env, FlagKey.applyUnsafe(flag), VariationKey.applyUnsafe(variation), user, 1, at)

  private def bought(id: String, user: String, amount: Option[Double], at: Long) =
    MetricEvent(id, env, purchase, user, amount, at)

  test("attribution: only events after the first exposure count, per user totals feed the statistics"):
    withStore: store =>
      val flag = FlagKey.applyUnsafe("attribution")
      for
        _ <- store.insertExposures(
          List(
            exposure("attribution", "alice", "on", at = 100),
            exposure("attribution", "bob", "on", at = 100),
            exposure("attribution", "carol", "off", at = 100)
          )
        )
        _ <- store.insertMetrics(
          List(
            bought("e1", "alice", Some(10), at = 150),
            bought("e2", "alice", Some(5), at = 160),
            bought("e3", "bob", Some(99), at = 50), // before exposure: not caused by the flag
            bought("e4", "carol", None, at = 200)   // no value: counts as 1
          )
        )
        stats <- store.variationStats(env, flag, purchase)
      yield assertEquals(
        stats,
        List(
          VariationStats("off", users = 1, conversions = 1, sum = 1, sumSquares = 1),
          VariationStats("on", users = 2, conversions = 1, sum = 15, sumSquares = 225)
        )
      )

  test("the earliest exposure wins regardless of delivery order, and redelivery is idempotent"):
    withStore: store =>
      val flag = FlagKey.applyUnsafe("ordering")
      for
        _ <- store.insertExposures(List(exposure("ordering", "dave", "on", at = 500)))
        _ <-
          store.insertExposures(List(exposure("ordering", "dave", "off", at = 300))) // earlier, arrives later
        _ <- store.insertExposures(List(exposure("ordering", "dave", "on", at = 900)))
        _ <-
          store.insertMetrics(List(bought("x1", "dave", Some(1), 1000), bought("x1", "dave", Some(1), 1000)))
        _      <- store.insertMetrics(List(bought("x1", "dave", Some(1), 1000)))
        counts <- store.exposureCounts(env, flag)
        stats  <- store.variationStats(env, flag, purchase)
      yield
        assertEquals(counts, Map("off" -> 1L))
        assertEquals(stats.map(s => (s.variation, s.sum)), List(("off", 1.0)))

  test("report endpoint compares variations against the control"):
    withStore: store =>
      val users     = (1 to 400).toList
      val exposures = users.map(i => exposure("report", s"u$i", if i % 2 == 0 then "on" else "off", at = 1))
      // "on" converts 40%, "off" 20%
      val purchases = users.collect:
        case i if i % 2 == 0 && i % 5 < 2   => bought(s"p$i", s"u$i", Some(20), at = 2)
        case i if i % 2 == 1 && i % 10 == 1 => bought(s"p$i", s"u$i", Some(20), at = 2)
      val routes = ReportRoutes(store).routes.orNotFound
      for
        _        <- store.insertExposures(exposures)
        _        <- store.insertMetrics(purchases)
        response <- routes.run(Request[IO](
          Method.GET,
          uri"/stats/v1/environments/prod/flags/report/metrics/purchase?weights=on:50,off:50"
        ))
        body     <- response.as[String]
        overview <- routes.run(Request[IO](
          Method.GET,
          uri"/stats/v1/environments/prod/flags/report"
        )).flatMap(_.as[String])
      yield
        assertEquals(response.status, Status.Ok, body)
        val report = decode[ExperimentReport](body).toOption.get
        assertEquals(report.control, "off")
        assertEquals(
          report.variations.map(v => (v.variation, v.users, v.conversions)),
          List(("off", 200L, 40L), ("on", 200L, 80L))
        )
        val on = report.comparisons.head
        assert(on.significant, on)
        assertEquals(on.conversion.relativeLift.map(l => math.round(l * 100)), Some(100L))
        assert(!report.srm.detected)
        assert(overview.contains("\"purchase\""), overview)

  test("bad weights and unknown experiments are client errors"):
    withStore: store =>
      val routes = ReportRoutes(store).routes.orNotFound
      for
        missing <-
          routes.run(Request[IO](Method.GET, uri"/stats/v1/environments/prod/flags/nothing/metrics/purchase"))
        bad <- routes.run(Request[IO](
          Method.GET,
          uri"/stats/v1/environments/prod/flags/report/metrics/purchase?weights=on"
        ))
      yield
        assertEquals(missing.status, Status.NotFound)
        assertEquals(bad.status, Status.BadRequest)
