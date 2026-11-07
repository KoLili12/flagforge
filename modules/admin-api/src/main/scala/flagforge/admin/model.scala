package flagforge.admin

import scala.util.control.NoStackTrace

/** Who performs a change. Recorded in the audit log. Real authentication comes later. */
final case class Actor(name: String)

object Actor:
  val Anonymous: Actor = Actor("anonymous")

/** Expected business errors. Anything else is a bug and results in a 500. */
sealed abstract class AdminError(msg: String) extends Exception(msg) with NoStackTrace

object AdminError:
  final case class NotFound(message: String)                       extends AdminError(message)
  final case class Conflict(message: String)                       extends AdminError(message)
  final case class Invalid(message: String, errors: List[String])  extends AdminError(message)
