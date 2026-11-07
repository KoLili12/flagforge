package flagforge.domain

import io.circe.Json
import io.circe.derivation.{ConfiguredCodec, ConfiguredEnumCodec}

/** Type of values a flag can serve. */
enum FlagKind derives ConfiguredEnumCodec:
  case Boolean, String, Number, Json

/** One of the values a flag can serve, e.g. `on -> true` or `v2 -> "new-pricing"`. */
final case class Variation(
    key: VariationKey,
    value: Json,
    name: Option[String] = None
) derives ConfiguredCodec

/** Environment-independent part of a flag: what it is and which values it can take. */
final case class FlagDefinition(
    key: FlagKey,
    name: String,
    kind: FlagKind,
    variations: List[Variation],
    salt: String,
    description: Option[String] = None,
    tags: List[String] = Nil,
    archived: Boolean = false
) derives ConfiguredCodec

/** The flag is served only if `flag` is on and serves `variation` for the same context. */
final case class Prerequisite(flag: FlagKey, variation: VariationKey) derives ConfiguredCodec

/** Individual targeting: contexts whose key is in `contextKeys` always get `variation`. */
final case class Target(variation: VariationKey, contextKeys: Set[String]) derives ConfiguredCodec

enum Operator derives ConfiguredEnumCodec:
  case In, Contains, StartsWith, EndsWith, Matches
  case LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual
  case SemVerEqual, SemVerLessThan, SemVerGreaterThan
  case InSegment

/** A single condition on a context attribute. Matches if the attribute matches ANY of `values`. For
  * `InSegment` the attribute is ignored and `values` are segment keys.
  */
final case class Clause(
    attribute: String,
    operator: Operator,
    values: List[Json],
    negate: Boolean = false
) derives ConfiguredCodec

/** Share of traffic in units of 1/[[Weights.Total]] (i.e. 1000 = 1%). */
final case class WeightedVariation(variation: VariationKey, weight: Int) derives ConfiguredCodec

object Weights:
  /** Sum of all weights in a rollout. 100 000 gives 0.001% granularity. */
  val Total: Int = 100_000

/** What to serve when a rule (or the fallthrough) is reached. */
enum Serve derives ConfiguredCodec:
  case Fixed(variation: VariationKey)

  /** Percentage rollout. Contexts are bucketed by `bucketBy` attribute (context key by default). */
  case Rollout(variations: List[WeightedVariation], bucketBy: Option[String] = None)

/** All clauses must match (logical AND) for the rule to apply. */
final case class Rule(
    id: RuleId,
    clauses: List[Clause],
    serve: Serve,
    description: Option[String] = None
) derives ConfiguredCodec

/** Per-environment targeting configuration of a flag. */
final case class FlagConfig(
    enabled: Boolean,
    offVariation: VariationKey,
    fallthrough: Serve,
    prerequisites: List[Prerequisite] = Nil,
    targets: List[Target] = Nil,
    rules: List[Rule] = Nil
) derives ConfiguredCodec

object FlagConfig:
  /** Configuration of a freshly created flag: disabled, serving the "false"-like variation. Once enabled it
    * serves the first other variation to everyone.
    */
  def initial(variations: List[Variation]): Option[FlagConfig] =
    for
      off <- variations.find(_.value == Json.False).orElse(variations.lastOption).map(_.key)
      on = variations.map(_.key).find(_ != off).getOrElse(off)
    yield FlagConfig(enabled = false, offVariation = off, fallthrough = Serve.Fixed(on))

/** Everything needed to evaluate a flag in one environment. This is what SDKs receive. */
final case class FeatureFlag(
    key: FlagKey,
    kind: FlagKind,
    variations: List[Variation],
    salt: String,
    version: Long,
    config: FlagConfig
) derives ConfiguredCodec:
  def variation(key: VariationKey): Option[Variation] = variations.find(_.key == key)

object FeatureFlag:
  def from(definition: FlagDefinition, config: FlagConfig, version: Long): FeatureFlag =
    FeatureFlag(definition.key, definition.kind, definition.variations, definition.salt, version, config)
