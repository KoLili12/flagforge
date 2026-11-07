package flagforge.domain

import io.circe.derivation.ConfiguredCodec

/** Change of the data plane state, published to the `flag-changes` Kafka topic through the transactional
  * outbox. The topic is log-compacted by [[ChangeEvent.compactionKey]], so it always holds the latest state
  * of every flag and segment and the data plane can bootstrap by reading it from the beginning.
  */
enum ChangeEvent derives ConfiguredCodec:
  case FlagUpdated(environment: EnvironmentKey, flag: FeatureFlag)
  case FlagArchived(environment: EnvironmentKey, flagKey: FlagKey)
  case SegmentUpdated(environment: EnvironmentKey, segment: Segment)

  def compactionKey: String = this match
    case FlagUpdated(env, flag)       => s"$env/flag/${flag.key}"
    case FlagArchived(env, key)       => s"$env/flag/$key"
    case SegmentUpdated(env, segment) => s"$env/segment/${segment.key}"

  def environmentKey: EnvironmentKey = this match
    case FlagUpdated(env, _)    => env
    case FlagArchived(env, _)   => env
    case SegmentUpdated(env, _) => env

object ChangeEvent:
  val Topic = "flagforge.flag-changes"
