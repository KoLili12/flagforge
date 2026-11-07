package flagforge.stats

import scala.util.Random

class HypothesisTestsSuite extends munit.FunSuite:

  private def close(actual: Double, expected: Double, tolerance: Double = 1e-9)(using munit.Location) =
    assert(math.abs(actual - expected) <= tolerance, s"$actual != $expected (±$tolerance)")

  // Reference values: scipy.stats.ttest_ind(b, a, equal_var=False) and a manual two-proportion z-test.

  test("Welch's t-test matches scipy"):
    val a = Seq(27.5, 21.0, 19.0, 23.6, 17.0, 17.9, 16.9, 20.1, 21.9, 22.6, 23.1, 19.6, 19.0, 21.7, 21.4)
    val b = Seq(27.1, 22.0, 20.8, 23.4, 23.4, 23.5, 25.8, 22.0, 24.8, 20.2, 21.9, 22.1, 22.9, 20.5, 24.4)
    val e = HypothesisTests.welch(VariationStats.fromValues("a", a), VariationStats.fromValues("b", b))
    close(e.statistic, 2.455356398286)
    close(e.pValue, 0.021378001463)
    close(e.ciLow, 0.349237066192, 1e-8)
    close(e.ciHigh, 3.984096267141, 1e-8)

  test("two-proportion z-test matches the reference"):
    val e = HypothesisTests.twoProportions(VariationStats("c", 1000, 200, 200, 200), VariationStats("t", 1000, 240, 240, 240))
    close(e.statistic, 2.159167585438)
    close(e.pValue, 0.030837167829)
    close(e.ciLow, 0.003732721356)
    close(e.ciHigh, 0.076267278644)
    close(e.relativeLift.get, 0.2)

  test("sample ratio mismatch"):
    val fine = HypothesisTests.sampleRatioMismatch(Map("a" -> 5100L, "b" -> 4900L), Map("a" -> 1.0, "b" -> 1.0))
    close(fine.chiSquared, 4.0)
    close(fine.pValue, 0.045500263896)
    assert(!fine.detected, "p = 0.045 is not an SRM at the 0.001 threshold")
    val broken = HypothesisTests.sampleRatioMismatch(Map("a" -> 5300L, "b" -> 4700L), Map("a" -> 50.0, "b" -> 50.0))
    close(broken.pValue / 1.97317529007539e-09, 1.0, 1e-6)
    assert(broken.detected)

  test("degenerate inputs never produce NaN"):
    val empty = VariationStats("e", 0, 0, 0, 0)
    val all   = VariationStats("x", 10, 10, 10, 10)
    List(
      HypothesisTests.twoProportions(empty, empty),
      HypothesisTests.twoProportions(all, all),
      HypothesisTests.welch(all, all)
    ).foreach(e => assertEquals(e.pValue, 1.0))

  /** Simulates experiments with a known truth: this is what makes the numbers trustworthy. */
  private def simulate(runs: Int, users: Int, controlRate: Double, treatmentRate: Double, seed: Long): Double =
    val random = Random(seed)
    def arm(name: String, rate: Double) =
      val conversions = (1 to users).count(_ => random.nextDouble() < rate).toLong
      VariationStats(name, users.toLong, conversions, conversions.toDouble, conversions.toDouble)
    val rejected = (1 to runs).count(_ =>
      HypothesisTests.twoProportions(arm("c", controlRate), arm("t", treatmentRate)).pValue < 0.05
    )
    rejected.toDouble / runs

  test("A/A tests: false positive rate is ~5%"):
    val rate = simulate(runs = 1000, users = 2000, controlRate = 0.1, treatmentRate = 0.1, seed = 42)
    assert(rate > 0.03 && rate < 0.07, s"false positive rate $rate")

  test("A/B tests: a real 10% → 13% effect is detected most of the time"):
    val power = simulate(runs = 300, users = 3000, controlRate = 0.10, treatmentRate = 0.13, seed = 7)
    assert(power > 0.85, s"power $power")

class AnalysisSuite extends munit.FunSuite:

  private val control = VariationStats("off", 5000, 500, 500, 500)
  private val better  = VariationStats("on", 5000, 650, 650, 650)

  test("compares treatments with the control"):
    val report = Analysis.report(List(control, better), "off").toOption.get
    assertEquals(report.comparisons.map(c => (c.variation, c.significant)), List(("on", true)))
    assertEquals(report.warnings, Nil)

  test("Bonferroni correction for several treatments"):
    val report = Analysis.report(List(control, better, better.copy(variation = "v3")), "off").toOption.get
    assertEquals(report.adjustedAlpha, 0.025)

  test("flags sample ratio mismatch and small samples"):
    val report = Analysis.report(List(control, better.copy(users = 4000), VariationStats("tiny", 10, 1, 1, 1)), "off")
    val warnings = report.toOption.get.warnings
    assert(warnings.exists(_.startsWith("Sample ratio mismatch")), warnings)
    assert(warnings.exists(_.contains("too early")), warnings)

  test("unknown control is an error"):
    assert(Analysis.report(List(control), "missing").isLeft)
