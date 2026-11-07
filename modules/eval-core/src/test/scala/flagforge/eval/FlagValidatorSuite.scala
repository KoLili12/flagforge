package flagforge.eval

import cats.syntax.all.*
import flagforge.domain.*
import flagforge.eval.Fixtures.*
import io.circe.Json

class FlagValidatorSuite extends munit.FunSuite:

  private def errors(
      flag: FeatureFlag,
      others: List[FeatureFlag] = Nil,
      segments: Set[SegmentKey] = Set.empty
  ) =
    FlagValidator
      .validateConfig(flag, others.map(f => f.key -> f).toMap, segments)
      .fold(_.toList, _ => List.empty[String])

  private def assertError(errs: List[String], fragment: String)(using munit.Location) =
    assert(errs.exists(_.contains(fragment)), s"expected an error containing '$fragment', got: $errs")

  test("a well-formed flag is valid"):
    val flag = boolFlag("f").configured(
      _.copy(
        rules = List(Rule(
          rid("r"),
          List(clause("country", Operator.In, str("RU"))),
          rollout(On -> 30_000, Off -> 70_000)
        ))
      )
    )
    assertEquals(errors(flag), Nil)

  test("boolean flags need exactly true and false"):
    val errs = FlagValidator
      .validateVariations(FlagKind.Boolean, List(Variation(On, Json.True), Variation(Off, Json.True)))
      .fold(_.toList, _ => List.empty[String])
    assertError(errs, "boolean flag")

  test("duplicate variation keys"):
    val errs = FlagValidator
      .validateVariations(FlagKind.String, List(Variation(On, str("a")), Variation(On, str("b"))))
      .fold(_.toList, _ => List.empty[String])
    assertError(errs, "duplicate variation keys")

  test("rollout weights must sum to 100%"):
    assertError(
      errors(boolFlag("f").configured(_.copy(fallthrough = rollout(On -> 10, Off -> 10)))),
      "must sum to"
    )

  test("unknown variations everywhere are reported, all at once"):
    val ghost = vk("ghost")
    val flag  = boolFlag("f").configured(
      _.copy(offVariation = ghost, targets = List(Target(ghost, Set("u"))), fallthrough = Serve.Fixed(ghost))
    )
    assertEquals(errors(flag).count(_.contains("unknown variation 'ghost'")), 3)

  test("clause values must fit the operator"):
    val bad = List(
      clause("age", Operator.GreaterThan, str("eighteen")),
      clause("version", Operator.SemVerEqual, str("latest")),
      clause("email", Operator.Matches, str("([")),
      clause("x", Operator.In)
    )
    val flag = boolFlag("f").configured(_.copy(rules = List(Rule(rid("r"), bad, Serve.Fixed(On)))))
    val errs = errors(flag)
    List(
      "numbers",
      "semantic versions",
      "regular expressions",
      "at least one value"
    ).foreach(assertError(errs, _))

  test("segment references must exist"):
    val rule = Rule(rid("r"), List(clause("", Operator.InSegment, str("beta"))), Serve.Fixed(On))
    val flag = boolFlag("f").configured(_.copy(rules = List(rule)))
    assertError(errors(flag), "unknown segment 'beta'")
    assertEquals(errors(flag, segments = Set(sk("beta"))), Nil)

  test("prerequisites must exist and reference a real variation"):
    val flag = boolFlag("f").configured(
      _.copy(prerequisites = List(Prerequisite(fk("ghost"), On), Prerequisite(fk("other"), vk("nope"))))
    )
    val errs = errors(flag, others = List(boolFlag("other")))
    assertError(errs, "'ghost' does not exist")
    assertError(errs, "has no variation 'nope'")

  test("prerequisite cycles are rejected at write time"):
    val a = boolFlag("a").configured(_.copy(prerequisites = List(Prerequisite(fk("b"), On))))
    val b = boolFlag("b").configured(_.copy(prerequisites = List(Prerequisite(fk("c"), On))))
    val c = boolFlag("c").configured(_.copy(prerequisites = List(Prerequisite(fk("a"), On))))
    assertError(errors(a, others = List(b, c)), "cycle: a -> b -> c -> a")

  test("segments cannot nest segments"):
    val segment =
      Segment(sk("s"), "S", rules = List(SegmentRule(List(clause("", Operator.InSegment, str("other"))))))
    assert(FlagValidator.validateSegment(segment).isInvalid)
