package flagforge.domain

import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*

class JsonFormatSuite extends munit.FunSuite:

  private def v(s: String) = VariationKey.applyUnsafe(s)

  test("Serve uses a flat 'type' discriminator"):
    val serve: Serve =
      Serve.Rollout(List(WeightedVariation(v("on"), 25_000), WeightedVariation(v("off"), 75_000)))
    val json = serve.asJson
    assertEquals(json.hcursor.get[String]("type"), Right("rollout"))
    assertEquals(json.as[Serve], Right(serve))

  test("operators are snake_case strings"):
    assertEquals((Operator.StartsWith: Operator).asJson, Json.fromString("starts_with"))
    assertEquals(Json.fromString("sem_ver_less_than").as[Operator], Right(Operator.SemVerLessThan))

  test("optional collections can be omitted in input"):
    val raw =
      """{
        |  "enabled": true,
        |  "offVariation": "off",
        |  "fallthrough": { "type": "fixed", "variation": "on" }
        |}""".stripMargin
    assertEquals(
      decode[FlagConfig](raw),
      Right(FlagConfig(enabled = true, offVariation = v("off"), fallthrough = Serve.Fixed(v("on"))))
    )

  test("full feature flag round-trips"):
    val flag = FeatureFlag(
      key = FlagKey.applyUnsafe("new-checkout"),
      kind = FlagKind.Boolean,
      variations = List(Variation(v("on"), Json.True), Variation(v("off"), Json.False)),
      salt = "s4lt",
      version = 3,
      config = FlagConfig(
        enabled = true,
        offVariation = v("off"),
        fallthrough = Serve.Fixed(v("off")),
        targets = List(Target(v("on"), Set("user-1"))),
        rules = List(
          Rule(
            RuleId.applyUnsafe("ru-users"),
            List(Clause("country", Operator.In, List(Json.fromString("RU")))),
            Serve.Fixed(v("on"))
          )
        )
      )
    )
    assertEquals(flag.asJson.as[FeatureFlag], Right(flag))

  test("evaluation reasons are discriminated"):
    val reason: Reason = Reason.RuleMatch(RuleId.applyUnsafe("r1"), 0, inRollout = true)
    assertEquals(reason.asJson.hcursor.get[String]("type"), Right("rule_match"))
    assertEquals((Reason.Off: Reason).asJson.as[Reason], Right(Reason.Off))
