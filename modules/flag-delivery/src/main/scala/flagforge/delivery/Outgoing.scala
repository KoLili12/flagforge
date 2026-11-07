package flagforge.delivery

import flagforge.domain.*
import io.circe.syntax.*
import org.http4s.ServerSentEvent
import org.http4s.ServerSentEvent.EventId

/** Messages of the SDK stream protocol:
  *
  *   - `put` — the full ruleset of the environment; sent on connect and whenever the client cannot be resumed;
  *   - `patch` — one [[ChangeEvent]]; applied by the SDK with the same version guards as on the server.
  *
  * The SSE `id` of every message is the change-feed offset, so `Last-Event-ID` lets a reconnecting client receive
  * only what it missed — from any delivery instance, because they all consume the same single-partition topic.
  */
enum Outgoing:
  case Put(offset: Long, ruleset: Ruleset)
  case Patch(offset: Long, event: ChangeEvent)

  def toSse: ServerSentEvent = this match
    case Put(offset, ruleset) =>
      ServerSentEvent(Some(ruleset.asJson.noSpaces), Some("put"), Some(EventId(offset.toString)))
    case Patch(offset, event) =>
      ServerSentEvent(Some(event.asJson.noSpaces), Some("patch"), Some(EventId(offset.toString)))

object Outgoing:

  /** What a client of `env` that has seen everything up to `cursor` must receive for the new `state`.
    *
    * @return
    *   the new cursor and the messages to send
    */
  def plan(env: EnvironmentKey, cursor: Option[Long], state: DeliveryState): (Option[Long], Vector[Outgoing]) =
    val fullSync = Vector(Put(state.offset, state.ruleset(env)))
    cursor match
      case Some(seen) if seen == state.offset => (cursor, Vector.empty)
      case Some(seen) =>
        val messages = state.changesSince(env, seen).fold(fullSync)(_.map(v => Patch(v.offset, v.event)))
        (Some(state.offset), messages)
      case None => (Some(state.offset), fullSync)
