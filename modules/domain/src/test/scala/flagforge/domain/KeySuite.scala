package flagforge.domain

import io.circe.Json
import io.circe.syntax.*

class KeySuite extends munit.FunSuite:

  test("accepts valid slugs"):
    List("new-checkout", "a", "v2", "pricing.algo_v3", "0-day").foreach: s =>
      assert(FlagKey.option(s).isDefined, s)

  test("rejects invalid slugs"):
    List("", "New-Checkout", "-leading-dash", "with space", "кириллица", "x" * 65).foreach: s =>
      assert(FlagKey.option(s).isEmpty, s)

  test("JSON codec round-trips and reports invalid keys"):
    val key = FlagKey.applyUnsafe("new-checkout")
    assertEquals(key.asJson, Json.fromString("new-checkout"))
    assertEquals(Json.fromString("new-checkout").as[FlagKey], Right(key))
    assert(Json.fromString("Bad Key").as[FlagKey].isLeft)
