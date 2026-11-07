package flagforge.delivery

import cats.Applicative
import cats.syntax.all.*

/** Persistent copy of the delivery state used for warm starts. */
trait SnapshotStore[F[_]]:
  def load: F[Option[Snapshot]]
  def save(snapshot: Snapshot): F[Unit]

object SnapshotStore:
  def noop[F[_]: Applicative]: SnapshotStore[F] = new:
    def load: F[Option[Snapshot]]          = none[Snapshot].pure[F]
    def save(snapshot: Snapshot): F[Unit] = ().pure[F]
