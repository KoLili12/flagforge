package flagforge.delivery

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.effect.Log
import io.circe.parser.decode
import io.circe.syntax.*
import org.typelevel.log4cats.LoggerFactory

/** Snapshot in a single Redis key, shared by all delivery instances.
  *
  * A new instance restores the state from it and seeks Kafka right after the snapshot offset instead of reading
  * the whole topic: startup time no longer grows with the number of flags × environments. Any instance may
  * overwrite it — every snapshot is a consistent state at its offset, and the feed is the same for everyone.
  */
object RedisSnapshotStore:

  val Key = "flagforge:delivery:snapshot"

  def resource[F[_]: Async: LoggerFactory](uri: String): Resource[F, SnapshotStore[F]] =
    val logger = LoggerFactory[F].getLoggerFromName("redis")
    given Log[F] = new Log[F]:
      def debug(msg: => String): F[Unit] = logger.debug(msg)
      def error(msg: => String): F[Unit] = logger.error(msg)
      def info(msg: => String): F[Unit]  = logger.info(msg)

    Redis[F]
      .utf8(uri)
      .map: redis =>
        new SnapshotStore[F]:
          def load: F[Option[Snapshot]] =
            redis
              .get(Key)
              .flatMap(_.flatTraverse(raw =>
                decode[Snapshot](raw) match
                  case Right(snapshot) => snapshot.some.pure[F]
                  case Left(error) => logger.warn(s"Ignoring unreadable snapshot: ${error.getMessage}").as(None)
              ))

          def save(snapshot: Snapshot): F[Unit] = redis.set(Key, snapshot.asJson.noSpaces)
