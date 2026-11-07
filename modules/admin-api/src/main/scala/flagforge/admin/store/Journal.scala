package flagforge.admin.store

import cats.MonadThrow
import org.typelevel.otel4s.trace.Tracer
import cats.syntax.all.*
import flagforge.admin.{Actor, AdminError}
import flagforge.admin.db.codecs.*
import flagforge.domain.{ChangeEvent, EnvironmentKey}
import io.circe.Json
import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.implicits.*

/** Audit log and outbox writes. Always called inside the transaction that performs the change. */
private[store] object Journal:

  private val insertAudit =
    sql"""INSERT INTO audit_log (environment, entity_type, entity_key, action, actor, payload)
          VALUES (${environmentKey.opt}, $text, $text, $text, $text, ${jsonb[Json]})""".command

  private val insertOutbox =
    sql"""INSERT INTO outbox (topic, key, payload, headers)
          VALUES ($text, $text, ${jsonb[ChangeEvent]}, ${jsonb[Map[String, String]]})""".command

  /** Outbox rows carry the current trace context, so the change can be followed into the data plane. */
  def record[F[_]: MonadThrow: Tracer](
      session: Session[F],
      actor: Actor,
      environment: Option[EnvironmentKey],
      entityType: String,
      entityKey: String,
      action: String,
      payload: Json,
      events: List[ChangeEvent]
  ): F[Unit] =
    Tracer[F].propagate(Map.empty[String, String]).flatMap: traceHeaders =>
      session.execute(insertAudit)((environment, entityType, entityKey, action, actor.name, payload)) *>
        events.traverse_(e =>
          session.execute(insertOutbox)((ChangeEvent.Topic, e.compactionKey, e, traceHeaders))
        )

  /** Turns a unique constraint violation into a [[AdminError.Conflict]]. */
  def conflictOnDuplicate[F[_]: MonadThrow, A](fa: F[A], message: => String): F[A] =
    fa.adaptError { case SqlState.UniqueViolation(_) => AdminError.Conflict(message) }
