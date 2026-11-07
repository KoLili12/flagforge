package flagforge.domain

import io.circe.Json
import io.circe.derivation.{ConfiguredCodec, ConfiguredEnumCodec}

enum ErrorKind derives ConfiguredEnumCodec:
  case FlagNotFound, MalformedFlag, PrerequisiteCycle

  /** SDK-side errors: no ruleset received yet / the served value has a different type than requested. */
  case ClientNotReady, WrongType

/** Why a particular variation was served. Invaluable for debugging targeting and for experiment analysis. */
enum Reason derives ConfiguredCodec:
  case Off
  case PrerequisiteFailed(prerequisite: FlagKey)
  case TargetMatch
  case RuleMatch(ruleId: RuleId, ruleIndex: Int, inRollout: Boolean)
  case Fallthrough(inRollout: Boolean)
  case Error(kind: ErrorKind, message: String)

/** Result of evaluating one flag for one context. `variation` and `value` are empty only on errors, in which
  * case the caller should use its own default value.
  */
final case class Evaluation(
    flag: FlagKey,
    variation: Option[VariationKey],
    value: Option[Json],
    reason: Reason,
    flagVersion: Option[Long]
) derives ConfiguredCodec:
  def isError: Boolean = reason match
    case _: Reason.Error => true
    case _               => false
