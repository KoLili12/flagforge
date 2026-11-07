package flagforge.admin.service

import flagforge.domain.*
import flagforge.domain.jsonConfiguration
import io.circe.derivation.ConfiguredCodec

final case class CreateFlagRequest(
    key: FlagKey,
    name: String,
    kind: FlagKind,
    variations: List[Variation],
    description: Option[String] = None,
    tags: List[String] = Nil
) derives ConfiguredCodec

/** `version` is the version the client has seen; the update is rejected if the flag changed since. */
final case class UpdateFlagConfigRequest(version: Long, config: FlagConfig) derives ConfiguredCodec

final case class ToggleFlagRequest(enabled: Boolean) derives ConfiguredCodec

/** `version` is the version the client has seen: `0` for a new segment. */
final case class SaveSegmentRequest(
    name: String,
    description: Option[String] = None,
    included: Set[String] = Set.empty,
    excluded: Set[String] = Set.empty,
    rules: List[SegmentRule] = Nil,
    version: Long = 0
) derives ConfiguredCodec:
  def toSegment(key: SegmentKey): Segment =
    Segment(key, name, description, included, excluded, rules, version)
