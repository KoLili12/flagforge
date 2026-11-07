package flagforge.domain

import io.circe.Json

class RulesetChangeSuite extends munit.FunSuite:

  private val prod   = EnvironmentKey.applyUnsafe("prod")
  private val dev    = EnvironmentKey.applyUnsafe("dev")
  private val on     = VariationKey.applyUnsafe("on")
  private val off    = VariationKey.applyUnsafe("off")
  private val key    = FlagKey.applyUnsafe("checkout")
  private val config = FlagConfig(enabled = true, offVariation = off, fallthrough = Serve.Fixed(on))

  private def flag(version: Long) =
    FeatureFlag(key, FlagKind.Boolean, List(Variation(on, Json.True), Variation(off, Json.False)), "s", version, config)

  test("newer versions are applied, stale and duplicate ones are ignored"):
    val v2 = Ruleset(prod).applyChange(ChangeEvent.FlagUpdated(prod, flag(2))).get
    assertEquals(v2.flags(key).version, 2L)
    assertEquals(v2.applyChange(ChangeEvent.FlagUpdated(prod, flag(1))), None)
    assertEquals(v2.applyChange(ChangeEvent.FlagUpdated(prod, flag(2))), None)
    assertEquals(v2.applyChange(ChangeEvent.FlagUpdated(prod, flag(3))).map(_.flags(key).version), Some(3L))

  test("events for other environments are ignored"):
    assertEquals(Ruleset(prod).applyChange(ChangeEvent.FlagUpdated(dev, flag(1))), None)

  test("archiving removes the flag once"):
    val withFlag = Ruleset(prod).withFlag(flag(1))
    val archived = withFlag.applyChange(ChangeEvent.FlagArchived(prod, key))
    assertEquals(archived.map(_.flags), Some(Map.empty))
    assertEquals(archived.flatMap(_.applyChange(ChangeEvent.FlagArchived(prod, key))), None)
