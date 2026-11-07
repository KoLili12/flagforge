package flagforge.eval

import flagforge.domain.Operator
import io.circe.Json

import scala.util.Try
import scala.util.matching.Regex

/** Semantics of clause operators on JSON values. Type mismatches never match (and never throw). */
object Operators:

  /** Does the context value `actual` satisfy `operator` against one clause value `expected`? `InSegment` is not
    * an attribute operator and is handled by the evaluator.
    */
  def matches(operator: Operator, actual: Json, expected: Json): Boolean = operator match
    case Operator.In                 => jsonEquals(actual, expected)
    case Operator.Contains           => strings(actual, expected)(_.contains(_))
    case Operator.StartsWith         => strings(actual, expected)(_.startsWith(_))
    case Operator.EndsWith           => strings(actual, expected)(_.endsWith(_))
    case Operator.Matches            => strings(actual, expected)((s, re) => regex(re).exists(_.findFirstIn(s).isDefined))
    case Operator.LessThan           => numbers(actual, expected)(_ < _)
    case Operator.LessThanOrEqual    => numbers(actual, expected)(_ <= _)
    case Operator.GreaterThan        => numbers(actual, expected)(_ > _)
    case Operator.GreaterThanOrEqual => numbers(actual, expected)(_ >= _)
    case Operator.SemVerEqual        => semvers(actual, expected)(_ == 0)
    case Operator.SemVerLessThan     => semvers(actual, expected)(_ < 0)
    case Operator.SemVerGreaterThan  => semvers(actual, expected)(_ > 0)
    case Operator.InSegment          => false

  def regex(pattern: String): Option[Regex] = Try(pattern.r).toOption

  /** Numbers are compared by value, so `1`, `1.0` and `1e0` are equal. */
  private def jsonEquals(a: Json, b: Json): Boolean =
    (a.asNumber, b.asNumber) match
      case (Some(x), Some(y)) => x.toBigDecimal.isDefined && x.toBigDecimal == y.toBigDecimal
      case _                  => a == b

  private def strings(a: Json, b: Json)(f: (String, String) => Boolean): Boolean =
    (a.asString, b.asString) match
      case (Some(x), Some(y)) => f(x, y)
      case _                  => false

  private def numbers(a: Json, b: Json)(f: (BigDecimal, BigDecimal) => Boolean): Boolean =
    (a.asNumber.flatMap(_.toBigDecimal), b.asNumber.flatMap(_.toBigDecimal)) match
      case (Some(x), Some(y)) => f(x, y)
      case _                  => false

  private def semvers(a: Json, b: Json)(f: Int => Boolean): Boolean =
    (a.asString.flatMap(SemVer.parse), b.asString.flatMap(SemVer.parse)) match
      case (Some(x), Some(y)) => f(Ordering[SemVer].compare(x, y))
      case _                  => false
