package flagforge.eval

import cats.data.{NonEmptyChain, ValidatedNec}
import cats.syntax.all.*
import flagforge.domain.*
import io.circe.Json

/** Write-time validation. Everything that passes here is guaranteed to evaluate without
  * [[ErrorKind.MalformedFlag]] or [[ErrorKind.PrerequisiteCycle]] errors.
  */
object FlagValidator:

  type Result[A] = ValidatedNec[String, A]

  def validateVariations(kind: FlagKind, variations: List[Variation]): Result[Unit] =
    val keys = variations.map(_.key)
    List(
      check(variations.nonEmpty, "a flag needs at least one variation"),
      check(keys.distinct.size == keys.size, s"duplicate variation keys: ${duplicates(keys).mkString(", ")}"),
      kind match
        case FlagKind.Boolean =>
          check(
            variations.map(_.value).toSet == Set(Json.True, Json.False) && variations.size == 2,
            "a boolean flag needs exactly two variations: true and false"
          )
        case FlagKind.String =>
          check(variations.forall(_.value.isString), "all variations of a string flag must be strings")
        case FlagKind.Number =>
          check(variations.forall(_.value.isNumber), "all variations of a number flag must be numbers")
        case FlagKind.Json => ().validNec
    ).combineAll

  /** @param flags
    *   all flags of the environment (the new version of `flag` may or may not be among them)
    * @param segments
    *   keys of all segments of the environment
    */
  def validateConfig(
      flag: FeatureFlag,
      flags: Map[FlagKey, FeatureFlag],
      segments: Set[SegmentKey]
  ): Result[FeatureFlag] =
    val config                                         = flag.config
    val variations                                     = flag.variations.map(_.key).toSet
    def knownVariation(v: VariationKey, where: String) =
      check(variations.contains(v), s"$where: unknown variation '$v'")

    val targetedKeys = config.targets.flatMap(_.contextKeys)
    val ruleIds      = config.rules.map(_.id)

    List(
      validateVariations(flag.kind, flag.variations),
      knownVariation(config.offVariation, "offVariation"),
      validateServe(config.fallthrough, variations, "fallthrough"),
      config.targets.traverse_(t => knownVariation(t.variation, "target")),
      check(
        targetedKeys.distinct.size == targetedKeys.size,
        s"context keys targeted more than once: ${duplicates(targetedKeys).mkString(", ")}"
      ),
      check(
        ruleIds.distinct.size == ruleIds.size,
        s"duplicate rule ids: ${duplicates(ruleIds).mkString(", ")}"
      ),
      config.rules.traverse_ { rule =>
        val where = s"rule '${rule.id}'"
        check(rule.clauses.nonEmpty, s"$where: needs at least one clause") |+|
          rule.clauses.traverse_(validateClause(_, segments, allowSegments = true, where)) |+|
          validateServe(rule.serve, variations, where)
      },
      validatePrerequisites(flag, flags)
    ).combineAll.as(flag)

  def validateSegment(segment: Segment): Result[Segment] =
    segment.rules.zipWithIndex
      .traverse_ { (rule, i) =>
        val where = s"segment rule #$i"
        check(rule.clauses.nonEmpty, s"$where: needs at least one clause") |+|
          rule.clauses.traverse_(validateClause(_, Set.empty, allowSegments = false, where))
      }
      .as(segment)

  private def validateServe(serve: Serve, variations: Set[VariationKey], where: String): Result[Unit] =
    serve match
      case Serve.Fixed(v)             => check(variations.contains(v), s"$where: unknown variation '$v'")
      case Serve.Rollout(weighted, _) =>
        val keys = weighted.map(_.variation)
        List(
          check(weighted.nonEmpty, s"$where: rollout needs at least one variation"),
          keys.traverse_(v => check(variations.contains(v), s"$where: unknown variation '$v'")),
          check(keys.distinct.size == keys.size, s"$where: duplicate variations in rollout"),
          check(weighted.forall(_.weight >= 0), s"$where: weights must be non-negative"),
          check(
            weighted.map(_.weight.toLong).sum == Weights.Total,
            s"$where: weights must sum to ${Weights.Total}, got ${weighted.map(_.weight.toLong).sum}"
          )
        ).combineAll

  private def validateClause(
      clause: Clause,
      segments: Set[SegmentKey],
      allowSegments: Boolean,
      where: String
  ): Result[Unit] =
    val prefix                                      = s"$where, clause on '${clause.attribute}'"
    def allValues(p: Json => Boolean, what: String) =
      check(clause.values.forall(p), s"$prefix: all values must be $what")

    val operatorSpecific = clause.operator match
      case Operator.In                                                 => ().validNec
      case Operator.Contains | Operator.StartsWith | Operator.EndsWith =>
        allValues(_.isString, "strings")
      case Operator.Matches =>
        allValues(_.asString.flatMap(Operators.regex).isDefined, "valid regular expressions")
      case Operator.LessThan | Operator.LessThanOrEqual | Operator.GreaterThan | Operator.GreaterThanOrEqual =>
        allValues(_.isNumber, "numbers")
      case Operator.SemVerEqual | Operator.SemVerLessThan | Operator.SemVerGreaterThan =>
        allValues(_.asString.flatMap(SemVer.parse).isDefined, "semantic versions")
      case Operator.InSegment =>
        if !allowSegments then s"$prefix: segments cannot reference other segments".invalidNec
        else
          clause.values.traverse_ { v =>
            v.asString.flatMap(SegmentKey.option) match
              case Some(key) => check(segments.contains(key), s"$prefix: unknown segment '$key'")
              case None      => s"$prefix: invalid segment key ${v.noSpaces}".invalidNec
          }

    check(clause.values.nonEmpty, s"$prefix: needs at least one value") |+| operatorSpecific

  private def validatePrerequisites(flag: FeatureFlag, flags: Map[FlagKey, FeatureFlag]): Result[Unit] =
    val all             = flags.updated(flag.key, flag)
    val prereqs         = flag.config.prerequisites
    val perPrerequisite = prereqs.traverse_ { p =>
      if p.flag == flag.key then s"flag cannot be its own prerequisite".invalidNec
      else
        all.get(p.flag) match
          case None        => s"prerequisite '${p.flag}' does not exist".invalidNec
          case Some(other) =>
            check(
              other.variation(p.variation).isDefined,
              s"prerequisite '${p.flag}' has no variation '${p.variation}'"
            )
    }
    val noDuplicates = check(
      prereqs.map(_.flag).distinct.size == prereqs.size,
      "duplicate prerequisites"
    )
    val noCycles = findCycle(flag.key, all) match
      case Some(path) => s"prerequisite cycle: ${path.mkString(" -> ")}".invalidNec
      case None       => ().validNec
    perPrerequisite |+| noDuplicates |+| noCycles

  /** DFS from `start` along prerequisite edges; returns the cycle path if `start` is reachable from itself.
    */
  private def findCycle(start: FlagKey, flags: Map[FlagKey, FeatureFlag]): Option[List[FlagKey]] =
    def edges(key: FlagKey) =
      flags.get(key).toList.flatMap(_.config.prerequisites.map(_.flag)).filterNot(_ == key)

    def go(current: FlagKey, path: List[FlagKey], seen: Set[FlagKey]): Option[List[FlagKey]] =
      edges(current).collectFirstSome: next =>
        if next == start then Some(start :: path.reverse ::: List(start))
        else if seen.contains(next) then None
        else go(next, next :: path, seen + next)

    go(start, Nil, Set(start))

  private def check(condition: Boolean, error: => String): Result[Unit] =
    if condition then ().validNec else error.invalidNec

  private def duplicates[A](as: List[A]): List[A] =
    as.groupBy(identity).collect { case (a, xs) if xs.size > 1 => a }.toList

  extension (errors: NonEmptyChain[String]) def render: String = errors.toList.mkString("; ")
