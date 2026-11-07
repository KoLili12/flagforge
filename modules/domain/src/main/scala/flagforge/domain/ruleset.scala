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

  /** Applies a change from the change feed. Returns `None` if the event is for another environment or is
    * stale (an older version than the one already known), so redelivered and reordered events are harmless.
    */
  def applyChange(event: ChangeEvent): Option[Ruleset] =
    if event.environmentKey != environment then None
    else
      event match
        case ChangeEvent.FlagUpdated(_, flag) =>
          Option.when(flags.get(flag.key).forall(_.version < flag.version))(withFlag(flag))
        case ChangeEvent.FlagArchived(_, key) =>
          Option.when(flags.contains(key))(withoutFlag(key))
        case ChangeEvent.SegmentUpdated(_, segment) =>
          Option.when(segments.get(segment.key).forall(_.version < segment.version))(withSegment(segment))
