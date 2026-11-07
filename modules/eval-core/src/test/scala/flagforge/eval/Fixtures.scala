package flagforge.eval

import flagforge.domain.*
import io.circe.Json

/** Small DSL for building test flags. */
object Fixtures:
  def fk(s: String): FlagKey      = FlagKey.applyUnsafe(s)
  def vk(s: String): VariationKey = VariationKey.applyUnsafe(s)
  def sk(s: String): SegmentKey   = SegmentKey.applyUnsafe(s)
  def rid(s: String): RuleId      = RuleId.applyUnsafe(s)

  val On: VariationKey  = vk("on")
  val Off: VariationKey = vk("off")

  def boolFlag(key: String, config: FlagConfig = FlagConfig(true, Off, Serve.Fixed(On))): FeatureFlag =
    FeatureFlag(fk(key), FlagKind.Boolean, List(Variation(On, Json.True), Variation(Off, Json.False)), "salt", 1, config)

  def stringFlag(key: String, values: String*): FeatureFlag =
    FeatureFlag(
      fk(key),
      FlagKind.String,
      values.toList.map(v => Variation(vk(v), Json.fromString(v))),
      "salt",
      1,
      FlagConfig(true, vk(values.head), Serve.Fixed(vk(values.head)))
    )

  def rollout(weights: (VariationKey, Int)*): Serve.Rollout =
    Serve.Rollout(weights.toList.map(WeightedVariation(_, _)))

  def clause(attribute: String, op: Operator, values: Json*): Clause =
    Clause(attribute, op, values.toList)

  def str(s: String): Json = Json.fromString(s)
  def num(n: Int): Json    = Json.fromInt(n)

  def ruleset(flags: FeatureFlag*): Ruleset =
    Ruleset(EnvironmentKey.applyUnsafe("test"), flags.map(f => f.key -> f).toMap)

  def ctx(key: String, attrs: (String, Json)*): EvaluationContext = EvaluationContext(key, attrs.toMap)

  extension (flag: FeatureFlag)
    def configured(f: FlagConfig => FlagConfig): FeatureFlag = flag.copy(config = f(flag.config))
