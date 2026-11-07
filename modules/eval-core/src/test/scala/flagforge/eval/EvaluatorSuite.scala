package flagforge.eval

import flagforge.domain.*
import flagforge.eval.Fixtures.*
import io.circe.Json

class EvaluatorSuite extends munit.FunSuite:

  private def eval(rs: Ruleset, flag: String, context: EvaluationContext = ctx("user-1")) =
    Evaluator.evaluate(rs, fk(flag), context)

  test("unknown flag is an error without a value"):
    val result = eval(ruleset(), "missing")
    assertEquals(result.value, None)
    assert(result.reason.isInstanceOf[Reason.Error])

  test("disabled flag serves the off variation"):
    val flag   = boolFlag("f").configured(_.copy(enabled = false))
    val result = eval(ruleset(flag), "f")
    assertEquals(result.variation, Some(Off))
    assertEquals(result.value, Some(Json.False))
    assertEquals(result.reason, Reason.Off)

  test("enabled flag without rules serves fallthrough"):
    val result = eval(ruleset(boolFlag("f")), "f")
    assertEquals(result.variation, Some(On))
    assertEquals(result.reason, Reason.Fallthrough(inRollout = false))
    assertEquals(result.flagVersion, Some(1L))

  test("individual targets win over rules"):
    val flag = boolFlag("f").configured(
      _.copy(
        targets = List(Target(Off, Set("vip"))),
        rules =
          List(Rule(rid("everyone"), List(clause("key", Operator.StartsWith, str(""))), Serve.Fixed(On)))
      )
    )
    assertEquals(eval(ruleset(flag), "f", ctx("vip")).reason, Reason.TargetMatch)
    assertEquals(eval(ruleset(flag), "f", ctx("vip")).variation, Some(Off))
    assertEquals(eval(ruleset(flag), "f", ctx("other")).variation, Some(On))

  test("first matching rule wins"):
    val flag = stringFlag("plan", "free", "pro", "enterprise").configured(
      _.copy(rules =
        List(
          Rule(
            rid("big"),
            List(clause("seats", Operator.GreaterThanOrEqual, num(100))),
            Serve.Fixed(vk("enterprise"))
          ),
          Rule(rid("paid"), List(clause("seats", Operator.GreaterThan, num(1))), Serve.Fixed(vk("pro")))
        )
      )
    )
    val rs = ruleset(flag)
    assertEquals(eval(rs, "plan", ctx("a", "seats" -> num(500))).value, Some(str("enterprise")))
    assertEquals(
      eval(rs, "plan", ctx("a", "seats" -> num(500))).reason,
      Reason.RuleMatch(rid("big"), 0, inRollout = false)
    )
    assertEquals(eval(rs, "plan", ctx("b", "seats" -> num(5))).value, Some(str("pro")))
    assertEquals(eval(rs, "plan", ctx("c", "seats" -> num(1))).value, Some(str("free")))

  test("all clauses of a rule must match"):
    val rule = Rule(
      rid("ru-mobile"),
      List(clause("country", Operator.In, str("RU"), str("KZ")), clause("platform", Operator.In, str("ios"))),
      Serve.Fixed(Off)
    )
    val rs = ruleset(boolFlag("f").configured(_.copy(rules = List(rule))))
    assertEquals(
      eval(rs, "f", ctx("a", "country" -> str("KZ"), "platform" -> str("ios"))).variation,
      Some(Off)
    )
    assertEquals(
      eval(rs, "f", ctx("a", "country" -> str("KZ"), "platform" -> str("web"))).variation,
      Some(On)
    )

  test("array attributes match if any element matches"):
    val rule = Rule(rid("r"), List(clause("groups", Operator.In, str("beta"))), Serve.Fixed(Off))
    val rs   = ruleset(boolFlag("f").configured(_.copy(rules = List(rule))))
    val user = ctx("a", "groups" -> Json.arr(str("staff"), str("beta")))
    assertEquals(eval(rs, "f", user).variation, Some(Off))

  test("negated clause inverts the match but a missing attribute never matches"):
    val rule = Rule(
      rid("not-ru"),
      List(clause("country", Operator.In, str("RU")).copy(negate = true)),
      Serve.Fixed(Off)
    )
    val rs = ruleset(boolFlag("f").configured(_.copy(rules = List(rule))))
    assertEquals(eval(rs, "f", ctx("a", "country" -> str("DE"))).variation, Some(Off))
    assertEquals(eval(rs, "f", ctx("a", "country" -> str("RU"))).variation, Some(On))
    assertEquals(eval(rs, "f", ctx("a")).variation, Some(On))

  test("segments: included, excluded and rule-based membership"):
    val beta = Segment(
      sk("beta"),
      "Beta testers",
      included = Set("alice"),
      excluded = Set("mallory"),
      rules = List(SegmentRule(List(clause("email", Operator.EndsWith, str("@flagforge.io")))))
    )
    val rule = Rule(rid("beta"), List(clause("", Operator.InSegment, str("beta"))), Serve.Fixed(Off))
    val rs   = ruleset(boolFlag("f").configured(_.copy(rules = List(rule)))).withSegment(beta)

    assertEquals(eval(rs, "f", ctx("alice")).variation, Some(Off))
    assertEquals(eval(rs, "f", ctx("bob", "email" -> str("bob@flagforge.io"))).variation, Some(Off))
    assertEquals(eval(rs, "f", ctx("mallory", "email" -> str("m@flagforge.io"))).variation, Some(On))
    assertEquals(eval(rs, "f", ctx("eve", "email" -> str("eve@example.com"))).variation, Some(On))

  test("prerequisite must be on and serve the expected variation"):
    val parent = boolFlag("parent")
    val child  = boolFlag("child").configured(_.copy(prerequisites = List(Prerequisite(fk("parent"), On))))
    assertEquals(eval(ruleset(parent, child), "child").variation, Some(On))

    val parentOff = parent.configured(_.copy(enabled = false))
    val failed    = eval(ruleset(parentOff, child), "child")
    assertEquals(failed.variation, Some(Off))
    assertEquals(failed.reason, Reason.PrerequisiteFailed(fk("parent")))

  test("missing prerequisite flag fails the prerequisite"):
    val child = boolFlag("child").configured(_.copy(prerequisites = List(Prerequisite(fk("ghost"), On))))
    assertEquals(eval(ruleset(child), "child").reason, Reason.PrerequisiteFailed(fk("ghost")))

  test("prerequisite cycles are detected instead of looping forever"):
    val a = boolFlag("a").configured(_.copy(prerequisites = List(Prerequisite(fk("b"), On))))
    val b = boolFlag("b").configured(_.copy(prerequisites = List(Prerequisite(fk("a"), On))))
    eval(ruleset(a, b), "a").reason match
      case Reason.Error(ErrorKind.PrerequisiteCycle, _) => ()
      case other                                        => fail(s"expected a cycle error, got $other")

  test("rollout reports inRollout and buckets by a custom attribute"):
    val flag = boolFlag("f").configured(
      _.copy(fallthrough =
        Serve.Rollout(List(WeightedVariation(On, 50_000), WeightedVariation(Off, 50_000)), Some("org"))
      )
    )
    val rs     = ruleset(flag)
    val first  = eval(rs, "f", ctx("u1", "org" -> str("acme")))
    val second = eval(rs, "f", ctx("u2", "org" -> str("acme")))
    assertEquals(first.reason, Reason.Fallthrough(inRollout = true))
    assertEquals(first.variation, second.variation, "same org must get the same variation")

  test("reference to an unknown variation is reported as malformed"):
    val flag = boolFlag("f").configured(_.copy(fallthrough = Serve.Fixed(vk("nope"))))
    eval(ruleset(flag), "f").reason match
      case Reason.Error(ErrorKind.MalformedFlag, _) => ()
      case other                                    => fail(s"expected malformed, got $other")

  test("evaluateAll evaluates every flag"):
    val rs  = ruleset(boolFlag("a"), boolFlag("b").configured(_.copy(enabled = false)))
    val all = Evaluator.evaluateAll(rs, ctx("u"))
    assertEquals(
      all.view.mapValues(_.value).toMap,
      Map(fk("a") -> Some(Json.True), fk("b") -> Some(Json.False))
    )
