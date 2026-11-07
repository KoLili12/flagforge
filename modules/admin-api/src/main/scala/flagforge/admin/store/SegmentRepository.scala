package flagforge.admin.store

import cats.effect.{MonadCancelThrow, Resource}
import cats.syntax.all.*
import flagforge.admin.{Actor, AdminError}
import flagforge.admin.db.codecs.*
import flagforge.domain.*
import io.circe.syntax.*
import org.typelevel.otel4s.trace.Tracer
import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.implicits.*

trait SegmentRepository[F[_]]:
  def list(env: EnvironmentKey): F[List[Segment]]
  def get(env: EnvironmentKey, key: SegmentKey): F[Option[Segment]]

  /** `segment.version` is the expected stored version: `0` creates a new segment, anything else updates an
    * existing one. Fails with `Conflict` on a version mismatch or if a "new" segment already exists.
    */
  def save(env: EnvironmentKey, segment: Segment, actor: Actor): F[Segment]

object SegmentRepository:

  private object Sql:
    val selectAll: Query[EnvironmentKey, Segment] =
      sql"SELECT segment, version FROM segments WHERE environment = $environmentKey ORDER BY key".query(
        segment
      )

    val selectOne: Query[(EnvironmentKey, SegmentKey), Segment] =
      sql"SELECT segment, version FROM segments WHERE environment = $environmentKey AND key = $segmentKey"
        .query(segment)

    val insert: Command[(EnvironmentKey, SegmentKey, Segment)] =
      sql"""INSERT INTO segments (environment, key, segment, version)
            VALUES ($environmentKey, $segmentKey, ${jsonb[Segment]}, 1)""".command

    val update: Query[(Segment, EnvironmentKey, SegmentKey, Long), Long] =
      sql"""UPDATE segments SET segment = ${jsonb[Segment]}, version = version + 1, updated_at = now()
            WHERE environment = $environmentKey AND key = $segmentKey AND version = $int8
            RETURNING version""".query(int8)

  def postgres[F[_]: MonadCancelThrow: Tracer](pool: Resource[F, Session[F]]): SegmentRepository[F] = new:

    def list(env: EnvironmentKey): F[List[Segment]] = pool.use(_.execute(Sql.selectAll)(env))

    def get(env: EnvironmentKey, key: SegmentKey): F[Option[Segment]] =
      pool.use(_.option(Sql.selectOne)((env, key)))

    def save(env: EnvironmentKey, segment: Segment, actor: Actor): F[Segment] = pool.use: s =>
      s.transaction.surround:
        val write: F[Long] =
          if segment.version == 0 then
            Journal
              .conflictOnDuplicate(
                s.execute(Sql.insert)((env, segment.key, segment)),
                s"segment '${segment.key}' already exists in '$env'"
              )
              .as(1L)
          else
            s.option(Sql.update)((segment, env, segment.key, segment.version))
              .flatMap(
                _.liftTo[F](
                  AdminError.Conflict(
                    s"segment '${segment.key}' in '$env' was modified concurrently: expected version ${segment.version}"
                  )
                )
              )
        for
          version <- write
          saved = segment.copy(version = version)
          _ <- Journal.record(
            s,
            actor,
            Some(env),
            "segment",
            segment.key.value,
            if version == 1 then "created" else "updated",
            saved.asJson,
            List(ChangeEvent.SegmentUpdated(env, saved))
          )
        yield saved
