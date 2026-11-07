package flagforge.stats

import io.circe.Codec

/** Sufficient statistics of one variation for one metric. Everything the tests need is derivable from these
  * five numbers, so they can be computed by a single SQL aggregation or incrementally on a stream.
  *
  * @param users
  *   exposed users (including those without any metric event)
  * @param conversions
  *   users with at least one metric event after their exposure
  * @param sum
  *   sum over users of the per-user metric value
  * @param sumSquares
  *   sum over users of the squared per-user metric value
  */
final case class VariationStats(
    variation: String,
    users: Long,
    conversions: Long,
    sum: Double,
    sumSquares: Double
) derives Codec.AsObject:
  def conversionRate: Double = if users == 0 then 0.0 else conversions.toDouble / users
  def mean: Double           = if users == 0 then 0.0 else sum / users

  /** Unbiased sample variance of the per-user value. */
  def variance: Double =
    if users < 2 then 0.0 else math.max(0.0, (sumSquares - sum * sum / users) / (users - 1))

object VariationStats:
  /** Per-user values; a user converted if their value is non-zero. */
  def fromValues(variation: String, values: Seq[Double]): VariationStats =
    VariationStats(
      variation,
      values.size.toLong,
      values.count(_ != 0).toLong,
      values.sum,
      values.map(v => v * v).sum
    )

/** Treatment vs control for one quantity, with a two-sided test and a confidence interval for the difference.
  */
final case class Estimate(
    control: Double,
    treatment: Double,
    difference: Double,
    relativeLift: Option[Double],
    ciLow: Double,
    ciHigh: Double,
    statistic: Double,
    pValue: Double
) derives Codec.AsObject

final case class SrmResult(
    chiSquared: Double,
    pValue: Double,
    expectedShares: Map[String, Double],
    observedShares: Map[String, Double],
    detected: Boolean
) derives Codec.AsObject

object HypothesisTests:

  /** Below this p-value the observed split is considered broken (the standard, deliberately strict
    * threshold).
    */
  val SrmThreshold = 0.001

  private def lift(control: Double, difference: Double): Option[Double] =
    Option.when(control != 0)(difference / control)

  /** Two-proportion z-test on conversion rates (pooled standard error for the test, unpooled for the CI). */
  def twoProportions(
      control: VariationStats,
      treatment: VariationStats,
      confidence: Double = 0.95
  ): Estimate =
    val (n1, n2) = (control.users.toDouble, treatment.users.toDouble)
    val (p1, p2) = (control.conversionRate, treatment.conversionRate)
    val diff     = p2 - p1
    val pooled   = (control.conversions + treatment.conversions) / (n1 + n2)
    val sePooled = math.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2))
    val se       = math.sqrt(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2)
    val q        = Distributions.normalQuantile(1 - (1 - confidence) / 2)
    if n1 == 0 || n2 == 0 || sePooled == 0 || sePooled.isNaN then
      Estimate(p1, p2, diff, lift(p1, diff), diff, diff, 0.0, 1.0)
    else
      val z = diff / sePooled
      Estimate(p1, p2, diff, lift(p1, diff), diff - q * se, diff + q * se, z, Distributions.normalTwoSided(z))

  /** Welch's t-test on means: does not assume equal variances, which rarely hold between variations. */
  def welch(control: VariationStats, treatment: VariationStats, confidence: Double = 0.95): Estimate =
    val (m1, m2) = (control.mean, treatment.mean)
    val diff     = m2 - m1
    val s1       = control.variance / control.users
    val s2       = treatment.variance / treatment.users
    val se       = math.sqrt(s1 + s2)
    if control.users < 2 || treatment.users < 2 || se == 0 || se.isNaN then
      Estimate(m1, m2, diff, lift(m1, diff), diff, diff, 0.0, 1.0)
    else
      val df =
        (s1 + s2) *
          (s1 + s2) /
          (s1 * s1 / (control.users - 1) + s2 * s2 / (treatment.users - 1))
      val t = diff / se
      val q = Distributions.studentQuantile(1 - (1 - confidence) / 2, df)
      Estimate(
        m1,
        m2,
        diff,
        lift(m1, diff),
        diff - q * se,
        diff + q * se,
        t,
        Distributions.studentTwoSided(t, df)
      )

  /** Chi-squared goodness-of-fit of the observed users per variation against the configured traffic split. A
    * mismatch means assignment or tracking is broken and the experiment results cannot be trusted.
    */
  def sampleRatioMismatch(observed: Map[String, Long], expectedWeights: Map[String, Double]): SrmResult =
    val total          = observed.values.sum.toDouble
    val weightTotal    = expectedWeights.values.sum
    val expected       = expectedWeights.view.mapValues(_ / weightTotal).toMap
    val observedShares =
      expected.keys.map(k => k -> (if total == 0 then 0.0 else observed.getOrElse(k, 0L) / total)).toMap
    if total == 0 || expected.size < 2 then SrmResult(0.0, 1.0, expected, observedShares, detected = false)
    else
      val chi2 = expected.toList.map: (variation, share) =>
        val e = share * total
        val o = observed.getOrElse(variation, 0L).toDouble
        if e == 0 then 0.0 else (o - e) * (o - e) / e
      .sum
      val p = Distributions.chiSquaredSf(chi2, expected.size - 1.0)
      SrmResult(chi2, p, expected, observedShares, detected = p < SrmThreshold)
