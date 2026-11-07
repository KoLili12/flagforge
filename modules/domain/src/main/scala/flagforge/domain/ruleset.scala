package flagforge.domain

import io.circe.derivation.ConfiguredCodec

/** Complete snapshot of one environment: every flag and segment. SDKs evaluate locally against it. */
final case class Ruleset(
    environment: EnvironmentKey,
    flags: Map[FlagKey, FeatureFlag] = Map.empty,
    segments: Map[SegmentKey, Segment] = Map.empty
) derives ConfiguredCodec:

  def withFlag(flag: FeatureFlag): Ruleset   = copy(flags = flags.updated(flag.key, flag))
  def withSegment(segment: Segment): Ruleset = copy(segments = segments.updated(segment.key, segment))
  def withoutFlag(key: FlagKey): Ruleset     = copy(flags = flags.removed(key))
