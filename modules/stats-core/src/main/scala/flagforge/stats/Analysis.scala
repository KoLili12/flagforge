package flagforge.stats

import io.circe.Codec

final case class Comparison(
    variation: String,
    conversion: Estimate,
    mean: Estimate,
    significant: Boolean
) derives Codec.AsObject

/** Result of an experiment for one metric.
  *
  * @param adjustedAlpha
  *   per-comparison significance level after the Bonferroni correction for comparing several treatments with
  *   the same control
  */
final case class ExperimentReport(
    control: String,
    alpha: Double,
    adjustedAlpha: Double,
    variations: List[VariationStats],
    comparisons: List[Comparison],
    srm: SrmResult,
    warnings: List[String]
) derives Codec.AsObject

object Analysis:

  val MinUsersPerVariation = 100

  /** Compares every variation with `control`.
    *
    * @param expectedWeights
    *   the configured traffic split, for the sample ratio mismatch check; equal split if empty
    */
  def report(
      stats: List[VariationStats],
      control: String,
      expectedWeights: Map[String, Double] = Map.empty,
      alpha: Double = 0.05
  ): Either[String, ExperimentReport] =
    stats.find(_.variation == control) match
      case None               => Left(s"control variation '$control' has no exposures")
      case Some(controlStats) =>
        val treatments    = stats.filterNot(_.variation == control)
        val adjustedAlpha = alpha / math.max(1, treatments.size)
        val confidence    = 1 - adjustedAlpha
        val comparisons   = treatments.map: t =>
          val conversion = HypothesisTests.twoProportions(controlStats, t, confidence)
          val mean       = HypothesisTests.welch(controlStats, t, confidence)
          Comparison(t.variation, conversion, mean, significant = conversion.pValue < adjustedAlpha)
        val weights =
          if expectedWeights.nonEmpty then expectedWeights else stats.map(_.variation -> 1.0).toMap
        val srm = HypothesisTests.sampleRatioMismatch(stats.map(s => s.variation -> s.users).toMap, weights)
        val warnings = List(
          Option.when(srm.detected)(
            f"Sample ratio mismatch (p = ${srm.pValue}%.2e): traffic split differs from the configuration, " +
              "results are not trustworthy"
          ),
          Option.when(stats.exists(_.users < MinUsersPerVariation))(
            s"Fewer than $MinUsersPerVariation users in some variations: too early to draw conclusions"
          )
        ).flatten
        Right(ExperimentReport(control, alpha, adjustedAlpha, stats, comparisons, srm, warnings))
