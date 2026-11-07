package flagforge.delivery

import flagforge.domain.*
import io.circe.Codec

/** An applied change together with its change-feed offset (the SSE event id). */
final case class Versioned(offset: Long, event: ChangeEvent)

/** Durable part of the state, stored in Redis for fast warm starts. */
final case class Snapshot(offset: Long, environments: Map[EnvironmentKey, Ruleset]) derives Codec

/** In-memory view of the change feed.
  *
  * @param offset
  *   offset of the last consumed record, `-1` before anything was consumed
  * @param recent
  *   the latest applied events, oldest first, used to resume SSE clients with only the missed changes
  * @param replayableFrom
  *   every applied event with `offset >= replayableFrom` is in `recent`
  */
final case class DeliveryState(
    offset: Long,
    environments: Map[EnvironmentKey, Ruleset],
    recent: Vector[Versioned],
    replayableFrom: Long,
    maxRecent: Int
):

  def ruleset(env: EnvironmentKey): Ruleset = environments.getOrElse(env, Ruleset(env))

  def snapshot: Snapshot = Snapshot(offset, environments)

  /** Applies a consumed record. Stale or duplicate events advance the offset but change nothing. */
  def applied(recordOffset: Long, event: ChangeEvent): DeliveryState =
    ruleset(event.environmentKey).applyChange(event) match
      case None => copy(offset = recordOffset)
      case Some(updated) =>
        val appended = recent :+ Versioned(recordOffset, event)
        val trimmed  = appended.takeRight(maxRecent)
        copy(
          offset = recordOffset,
          environments = environments.updated(event.environmentKey, updated),
          recent = trimmed,
          replayableFrom = if trimmed.size < appended.size then trimmed.head.offset else replayableFrom
        )

  /** A record that could not be decoded: only moves the offset. */
  def skipped(recordOffset: Long): DeliveryState = copy(offset = recordOffset)

  /** Changes of `env` after `lastSeen`, or `None` if they cannot be reconstructed and the client needs the full
    * ruleset again (it is too far behind, or comes from a different feed, e.g. after the topic was recreated).
    */
  def changesSince(env: EnvironmentKey, lastSeen: Long): Option[Vector[Versioned]] =
    if lastSeen > offset || lastSeen + 1 < replayableFrom then None
    else Some(recent.filter(v => v.offset > lastSeen && v.event.environmentKey == env))

object DeliveryState:
  val DefaultMaxRecent = 1000

  def empty(maxRecent: Int = DefaultMaxRecent): DeliveryState =
    DeliveryState(offset = -1, Map.empty, Vector.empty, replayableFrom = 0, maxRecent)

  def fromSnapshot(snapshot: Snapshot, maxRecent: Int = DefaultMaxRecent): DeliveryState =
    DeliveryState(snapshot.offset, snapshot.environments, Vector.empty, snapshot.offset + 1, maxRecent)
