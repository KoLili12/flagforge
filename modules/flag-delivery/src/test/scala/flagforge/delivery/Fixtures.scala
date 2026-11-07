package flagforge.delivery

import flagforge.domain.*
import io.circe.Json

object Fixtures:
  val prod: EnvironmentKey = EnvironmentKey.applyUnsafe("prod")
  val dev: EnvironmentKey  = EnvironmentKey.applyUnsafe("dev")
  val on: VariationKey     = VariationKey.applyUnsafe("on")
  val off: VariationKey    = VariationKey.applyUnsafe("off")

  def flag(key: String, version: Long, enabled: Boolean = true): FeatureFlag =
    FeatureFlag(
      FlagKey.applyUnsafe(key),
      FlagKind.Boolean,
      List(Variation(on, Json.True), Variation(off, Json.False)),
      "salt",
      version,
      FlagConfig(enabled = enabled, offVariation = off, fallthrough = Serve.Fixed(on))
    )

  def updated(env: EnvironmentKey, key: String, version: Long, enabled: Boolean = true): ChangeEvent =
    ChangeEvent.FlagUpdated(env, flag(key, version, enabled))

  /** Applies events at consecutive offsets starting from `from`. */
  def feed(state: DeliveryState, from: Long, events: ChangeEvent*): DeliveryState =
    events.zipWithIndex.foldLeft(state) { case (s, (e, i)) => s.applied(from + i, e) }
