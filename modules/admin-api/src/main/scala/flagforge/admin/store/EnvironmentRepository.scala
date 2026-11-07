package flagforge.admin.store

import cats.effect.{MonadCancelThrow, Resource}
import cats.syntax.all.*
import flagforge.admin.Actor
import flagforge.admin.db.codecs.*
import flagforge.domain.*
import io.circe.syntax.*
import org.typelevel.otel4s.trace.Tracer
import skunk.*
import skunk.implicits.*

trait EnvironmentRepository[F[_]]:
  def list: F[List[Environment]]
  def get(key: EnvironmentKey): F[Option[Environment]]

  /** Creates the environment together with an initial (disabled) config for every existing flag. */
  def create(environment: Environment, actor: Actor): F[Unit]

object EnvironmentRepository:

  private object Sql:
    val selectAll: Query[Void, Environment] =
      sql"SELECT key, name FROM environments ORDER BY key".query(environment)

    val selectOne: Query[EnvironmentKey, Environment] =
      sql"SELECT key, name FROM environments WHERE key = $environmentKey".query(environment)

    val insert: Command[Environment] =
      sql"INSERT INTO environments (key, name) VALUES ($environment)".command

  def postgres[F[_]: MonadCancelThrow: Tracer](pool: Resource[F, Session[F]]): EnvironmentRepository[F] = new:

    def list: F[List[Environment]] = pool.use(_.execute(Sql.selectAll))

    def get(key: EnvironmentKey): F[Option[Environment]] = pool.use(_.option(Sql.selectOne)(key))

    def create(env: Environment, actor: Actor): F[Unit] = pool.use: s =>
      s.transaction.surround:
        for
          _ <- Journal.conflictOnDuplicate(
            s.execute(Sql.insert)(env),
            s"environment '${env.key}' already exists"
          )
          flags   <- FlagRepository.Sql.definitions(s, includeArchived = true)
          created <- flags.flatTraverse: definition =>
            FlagConfig.initial(definition.variations).toList.traverse: config =>
              s.execute(FlagRepository.Sql.insertConfig)((definition.key, env.key, config))
                .as(definition -> FeatureFlag.from(definition, config, version = 1))
          events = created.collect:
            case (definition, flag) if !definition.archived => ChangeEvent.FlagUpdated(env.key, flag)
          _ <- Journal.record(
            s,
            actor,
            Some(env.key),
            "environment",
            env.key.value,
            "created",
            env.asJson,
            events
          )
        yield ()
