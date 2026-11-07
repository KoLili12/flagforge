package flagforge.eval

import flagforge.domain.*
import io.circe.Json

/** Pure flag evaluation. The algorithm, in order:
  *
  *   1. flag is off → `offVariation`
  *   2. a prerequisite is not met → `offVariation`
  *   3. context key is individually targeted → that variation
  *   4. first matching rule → its serve (fixed variation or percentage rollout)
  *   5. otherwise → `fallthrough`
  *
  * Never throws: malformed configuration and prerequisite cycles are reported as [[Reason.Error]].
  */
object Evaluator:

  def evaluate(ruleset: Ruleset, flagKey: FlagKey, context: EvaluationContext): Evaluation =
    ruleset.flags.get(flagKey) match
      case Some(flag) => evaluateFlag(ruleset, flag, context, visited = Set(flag.key))
      case None       => error(flagKey, None, ErrorKind.FlagNotFound, s"flag '$flagKey' does not exist")

  def evaluateAll(ruleset: Ruleset, context: EvaluationContext): Map[FlagKey, Evaluation] =
    ruleset.flags.view.mapValues(flag => evaluateFlag(ruleset, flag, context, Set(flag.key))).toMap

  private def evaluateFlag(
      ruleset: Ruleset,
      flag: FeatureFlag,
      context: EvaluationContext,
      visited: Set[FlagKey]
  ): Evaluation =
    val config = flag.config

    def serveFixed(variation: VariationKey, reason: Reason): Evaluation =
      flag.variation(variation) match
        case Some(v) => Evaluation(flag.key, Some(v.key), Some(v.value), reason, Some(flag.version))
        case None =>
          error(flag.key, Some(flag.version), ErrorKind.MalformedFlag, s"unknown variation '$variation'")

    def serve(s: Serve, reason: Boolean => Reason): Evaluation =
      resolve(flag, s, context) match
        case Some(variation) => serveFixed(variation, reason(s.isInstanceOf[Serve.Rollout]))
        case None            => error(flag.key, Some(flag.version), ErrorKind.MalformedFlag, "empty rollout")

    if !config.enabled then serveFixed(config.offVariation, Reason.Off)
    else
      checkPrerequisites(ruleset, flag, context, visited) match
        case Left(cycle)        => cycle
        case Right(Some(failed)) => serveFixed(config.offVariation, Reason.PrerequisiteFailed(failed))
        case Right(None) =>
          config.targets.find(_.contextKeys.contains(context.key)) match
            case Some(target) => serveFixed(target.variation, Reason.TargetMatch)
            case None =>
              config.rules.zipWithIndex.find((rule, _) => rule.clauses.forall(clauseMatches(ruleset, _, context))) match
                case Some((rule, index)) => serve(rule.serve, Reason.RuleMatch(rule.id, index, _))
                case None                => serve(config.fallthrough, Reason.Fallthrough(_))

  /** `Left` — a prerequisite cycle was detected; `Right(Some(key))` — the first failed prerequisite. */
  private def checkPrerequisites(
      ruleset: Ruleset,
      flag: FeatureFlag,
      context: EvaluationContext,
      visited: Set[FlagKey]
  ): Either[Evaluation, Option[FlagKey]] =
    flag.config.prerequisites.foldLeft[Either[Evaluation, Option[FlagKey]]](Right(None)):
      case (Right(None), prerequisite) =>
        def cycle = error(
          flag.key,
          Some(flag.version),
          ErrorKind.PrerequisiteCycle,
          s"prerequisite cycle through '${prerequisite.flag}'"
        )
        if visited.contains(prerequisite.flag) then Left(cycle)
        else
          ruleset.flags.get(prerequisite.flag) match
            case None => Right(Some(prerequisite.flag))
            case Some(prerequisiteFlag) =>
              val result = evaluateFlag(ruleset, prerequisiteFlag, context, visited + prerequisite.flag)
              result.reason match
                case Reason.Error(ErrorKind.PrerequisiteCycle, _) => Left(cycle)
                case _ if prerequisiteFlag.config.enabled && result.variation.contains(prerequisite.variation) =>
                  Right(None)
                case _ => Right(Some(prerequisite.flag))
      case (done, _) => done

  private def resolve(flag: FeatureFlag, serve: Serve, context: EvaluationContext): Option[VariationKey] =
    serve match
      case Serve.Fixed(variation) => Some(variation)
      case Serve.Rollout(variations, bucketBy) =>
        val bucketValue = bucketBy.flatMap(context.attribute).flatMap(bucketString).getOrElse(context.key)
        Bucketing.pick(variations, Bucketing.bucket(flag.key.value, flag.salt, bucketValue))

  private def bucketString(json: Json): Option[String] =
    json.asString.orElse(json.asNumber.map(_.toString))

  /** A missing attribute never matches, even for negated clauses. */
  private[eval] def clauseMatches(
      ruleset: Ruleset,
      clause: Clause,
      context: EvaluationContext,
      allowSegments: Boolean = true
  ): Boolean =
    val matched: Option[Boolean] =
      if clause.operator == Operator.InSegment then
        Some(allowSegments && clause.values.exists(inSegment(ruleset, _, context)))
      else
        context.attribute(clause.attribute).map: actual =>
          val candidates = actual.asArray.getOrElse(Vector(actual))
          candidates.exists(a => clause.values.exists(Operators.matches(clause.operator, a, _)))
    matched.exists(_ != clause.negate)

  private def inSegment(ruleset: Ruleset, segmentKey: Json, context: EvaluationContext): Boolean =
    segmentKey.asString
      .flatMap(SegmentKey.option)
      .flatMap(ruleset.segments.get)
      .exists: segment =>
        if segment.included.contains(context.key) then true
        else if segment.excluded.contains(context.key) then false
        else
          segment.rules.exists(
            _.clauses.forall(clauseMatches(ruleset, _, context, allowSegments = false))
          )

  private def error(flag: FlagKey, version: Option[Long], kind: ErrorKind, message: String): Evaluation =
    Evaluation(flag, None, None, Reason.Error(kind, message), version)
