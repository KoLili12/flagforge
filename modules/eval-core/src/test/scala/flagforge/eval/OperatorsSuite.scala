package flagforge.eval

import flagforge.domain.Operator
import flagforge.eval.Fixtures.*
import io.circe.Json

class OperatorsSuite extends munit.FunSuite:

  private def check(op: Operator, actual: Json, expected: Json, result: Boolean)(using munit.Location) =
    assertEquals(Operators.matches(op, actual, expected), result, s"$op(${actual.noSpaces}, ${expected.noSpaces})")

  test("in compares numbers by value"):
    check(Operator.In, Json.fromInt(1), Json.fromDoubleOrNull(1.0), true)
    check(Operator.In, str("1"), Json.fromInt(1), false)

  test("string operators"):
    check(Operator.Contains, str("hello world"), str("o w"), true)
    check(Operator.StartsWith, str("hello"), str("he"), true)
    check(Operator.EndsWith, str("hello"), str("he"), false)
    check(Operator.StartsWith, num(10), str("1"), false)

  test("regex operator tolerates invalid patterns"):
    check(Operator.Matches, str("user-42"), str("^user-\\d+$"), true)
    check(Operator.Matches, str("user-42"), str("([unclosed"), false)

  test("numeric comparisons"):
    check(Operator.LessThan, num(1), num(2), true)
    check(Operator.LessThanOrEqual, num(2), num(2), true)
    check(Operator.GreaterThan, Json.fromDoubleOrNull(2.5), num(2), true)
    check(Operator.GreaterThanOrEqual, str("3"), num(2), false)

  test("semantic versions"):
    check(Operator.SemVerGreaterThan, str("2.10.0"), str("2.9.1"), true)
    check(Operator.SemVerEqual, str("v2"), str("2.0.0"), true)
    check(Operator.SemVerLessThan, str("1.0.0-rc.1"), str("1.0.0"), true)
    check(Operator.SemVerLessThan, str("1.0.0-alpha"), str("1.0.0-alpha.1"), true)
    check(Operator.SemVerLessThan, str("1.0.0-alpha.2"), str("1.0.0-alpha.10"), true)
    check(Operator.SemVerLessThan, str("1.0.0-beta"), str("1.0.0-alpha"), false)
    check(Operator.SemVerEqual, str("not-a-version"), str("1.0.0"), false)

  test("semver ignores build metadata"):
    assertEquals(SemVer.parse("1.2.3+build.7"), SemVer.parse("1.2.3"))
