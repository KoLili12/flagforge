package flagforge.delivery

import flagforge.delivery.Fixtures.*
import flagforge.domain.*

class DeliveryStateSuite extends munit.FunSuite:

  test("events build per-environment rulesets"):
    val state =
      feed(DeliveryState.empty(), 0, updated(prod, "a", 1), updated(dev, "a", 1), updated(prod, "b", 1))
    assertEquals(state.offset, 2L)
    assertEquals(state.ruleset(prod).flags.keySet.map(k => k.value: String), Set("a", "b"))
    assertEquals(state.ruleset(dev).flags.keySet.map(k => k.value: String), Set("a"))
    assertEquals(state.ruleset(EnvironmentKey.applyUnsafe("unknown")).flags, Map.empty)

  test("stale events move the offset but are not replayed"):
    val state = feed(DeliveryState.empty(), 0, updated(prod, "a", 2), updated(prod, "a", 1))
    assertEquals(state.offset, 1L)
    assertEquals(state.ruleset(prod).flags.values.map(_.version).toList, List(2L))
    assertEquals(state.recent.map(_.offset), Vector(0L))

  test("changesSince returns only the missed changes of one environment"):
    val state =
      feed(DeliveryState.empty(), 0, updated(prod, "a", 1), updated(dev, "a", 1), updated(prod, "a", 2))
    assertEquals(state.changesSince(prod, 0).map(_.map(_.offset)), Some(Vector(2L)))
    assertEquals(state.changesSince(prod, 2), Some(Vector.empty))

  test("a client from the future (e.g. recreated topic) needs a full resync"):
    val state = feed(DeliveryState.empty(), 0, updated(prod, "a", 1))
    assertEquals(state.changesSince(prod, 42), None)

  test("clients that fell out of the retained window need a full resync"):
    val state = feed(DeliveryState.empty(maxRecent = 2), 0, (1 to 5).map(v => updated(prod, "a", v))*)
    assertEquals(state.recent.map(_.offset), Vector(3L, 4L))
    assertEquals(state.replayableFrom, 3L)
    assertEquals(state.changesSince(prod, 1), None)
    assertEquals(state.changesSince(prod, 2).map(_.map(_.offset)), Some(Vector(3L, 4L)))

  test("a state restored from a snapshot can resume clients that are up to date"):
    val original = feed(DeliveryState.empty(), 0, updated(prod, "a", 1), updated(prod, "b", 1))
    val restored = DeliveryState.fromSnapshot(original.snapshot)
    assertEquals(restored.ruleset(prod), original.ruleset(prod))
    assertEquals(restored.changesSince(prod, 1), Some(Vector.empty))
    assertEquals(restored.changesSince(prod, 0), None)
    val next = restored.applied(2, updated(prod, "a", 2))
    assertEquals(next.changesSince(prod, 1).map(_.map(_.offset)), Some(Vector(2L)))
