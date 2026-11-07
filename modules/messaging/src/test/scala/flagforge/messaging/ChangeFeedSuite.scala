package flagforge.messaging

import flagforge.domain.*

class ChangeFeedSuite extends munit.FunSuite:

  test("events survive a round trip through the wire format"):
    val event: ChangeEvent =
      ChangeEvent.FlagArchived(EnvironmentKey.applyUnsafe("prod"), FlagKey.applyUnsafe("old"))
    assertEquals(ChangeFeed.decodeEvent(ChangeFeed.encode(event)), Right(event))

  test("garbage is reported, not thrown"):
    assert(ChangeFeed.decodeEvent("""{"type":"unknown"}""").isLeft)
    assert(ChangeFeed.decodeEvent("not json").isLeft)
