package flagforge.stats

import flagforge.stats.Distributions.*

/** Reference values computed with scipy 1.13. */
class DistributionsSuite extends munit.FunSuite:

  private def close(actual: Double, expected: Double, tolerance: Double = 1e-9)(using munit.Location) =
    assert(math.abs(actual - expected) <= tolerance, s"$actual != $expected (±$tolerance)")

  test("normal CDF and quantile"):
    close(normalCdf(0), 0.5)
    close(normalCdf(1.959963984540), 0.975)
    close(normalQuantile(0.975), 1.959963984540)
    close(normalQuantile(1e-6), -4.753424308823)
    close(normalQuantile(0.5), 0.0)

  test("normal survival function stays accurate in the far tail"):
    close(normalSf(8) / 6.220961e-16, 1.0, 1e-5)

  test("Student t"):
    close(studentCdf(2.228138852, 10), 0.975)
    close(studentCdf(1, 1), 0.75) // Cauchy
    close(studentQuantile(0.975, 24.5), 2.061671879347, 1e-8)

  test("chi-squared survival function"):
    close(chiSquaredSf(7.5, 3), 0.057558451973)
    close(chiSquaredSf(5, 2), math.exp(-2.5))

  test("regularized incomplete beta"):
    close(betaI(2.5, 4, 0.3), 0.352197585907)
    close(betaI(3, 3, 0.5), 0.5)
