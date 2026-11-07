package flagforge.domain

import io.circe.derivation.ConfiguredCodec

/** Isolated set of flag configurations, e.g. `dev`, `staging`, `production`. */
final case class Environment(key: EnvironmentKey, name: String) derives ConfiguredCodec
