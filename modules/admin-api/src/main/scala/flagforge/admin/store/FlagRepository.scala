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
import skunk.data.Completion
import skunk.codec.all.*
import skunk.implicits.*

trait FlagRepository[F[_]]:
  def listDefinitions(includeArchived: Boolean): F[List[FlagDefinition]]
  def getDefinition(key: FlagKey): F[Option[FlagDefinition]]

  /** Inserts the flag and `initial` config into every environment. Fails with `Conflict` if the key is taken.
    */
  def create(definition: FlagDefinition, initial: FlagConfig, actor: Actor): F[Unit]

  /** Hides the flag from all environments. Archived flags keep their key reserved. */
  def archive(key: FlagKey, actor: Actor): F[Boolean]

  /** Active (non-archived) flags of an environment. */
  def list(env: EnvironmentKey): F[List[FeatureFlag]]
  def get(env: EnvironmentKey, key: FlagKey): F[Option[FeatureFlag]]

  /** Optimistic update: fails with `Conflict` unless the stored version equals `expectedVersion`. */
  def updateConfig(
      env: EnvironmentKey,
      key: FlagKey,
      expectedVersion: Long,
      config: FlagConfig,
      actor: Actor
  ): F[FeatureFlag]

object FlagRepository:

  private[store] object Sql:
    private val definitionColumns = sql"key, name, kind, variations, salt, description, tags, archived"

    val selectDefinitions: Query[Boolean, FlagDefinition] =
      sql"SELECT $definitionColumns FROM flags WHERE NOT archived OR $bool ORDER BY key".query(flagDefinition)

    val selectDefinition: Query[FlagKey, FlagDefinition] =
      sql"SELECT $definitionColumns FROM flags WHERE key = $flagKey".query(flagDefinition)

    val insertDefinition: Command[FlagDefinition] =
      sql"INSERT INTO flags ($definitionColumns) VALUES ($flagDefinition)".command

    val archive: Command[FlagKey] =
      sql"UPDATE flags SET archived = true, updated_at = now() WHERE key = $flagKey AND NOT archived".command

    val insertConfig: Command[(FlagKey, EnvironmentKey, FlagConfig)] =
      sql"""INSERT INTO flag_configs (flag_key, environment, config, version)
            VALUES ($flagKey, $environmentKey, ${jsonb[FlagConfig]}, 1)""".command

    private val flagSelect =
      sql"""SELECT f.key, f.kind, f.variations, f.salt, c.version, c.config
            FROM flags f JOIN flag_configs c ON c.flag_key = f.key"""

    val selectFlags: Query[EnvironmentKey, FeatureFlag] =
      sql"$flagSelect WHERE c.environment = $environmentKey AND NOT f.archived ORDER BY f.key".query(
        featureFlag
      )

    val selectFlag: Query[(EnvironmentKey, FlagKey), FeatureFlag] =
      sql"$flagSelect WHERE c.environment = $environmentKey AND f.key = $flagKey AND NOT f.archived"
        .query(featureFlag)

    val selectEnvironments: Query[Void, EnvironmentKey] =
      sql"SELECT key FROM environments".query(environmentKey)

    val updateConfig: Query[(FlagConfig, EnvironmentKey, FlagKey, Long), Long] =
      sql"""UPDATE flag_configs
            SET config = ${jsonb[FlagConfig]}, version = version + 1, updated_at = now()
            WHERE environment = $environmentKey AND flag_key = $flagKey AND version = $int8
            RETURNING version""".query(int8)

    def definitions[F[_]](s: Session[F], includeArchived: Boolean): F[List[FlagDefinition]] =
      s.execute(selectDefinitions)(includeArchived)

  def postgres[F[_]: MonadCancelThrow: Tracer](pool: Resource[F, Session[F]]): FlagRepository[F] = new:

    def listDefinitions(includeArchived: Boolean): F[List[FlagDefinition]] =
      pool.use(Sql.definitions(_, includeArchived))

    def getDefinition(key: FlagKey): F[Option[FlagDefinition]] =
      pool.use(_.option(Sql.selectDefinition)(key))

    def create(definition: FlagDefinition, initial: FlagConfig, actor: Actor): F[Unit] = pool.use: s =>
      s.transaction.surround:
        for
          _ <- Journal.conflictOnDuplicate(
            s.execute(Sql.insertDefinition)(definition),
            s"flag '${definition.key}' already exists"
          )
          envs <- s.execute(Sql.selectEnvironments)
          _    <- envs.traverse_(env => s.execute(Sql.insertConfig)((definition.key, env, initial)))
          flag   = FeatureFlag.from(definition, initial, version = 1)
          events = envs.map(ChangeEvent.FlagUpdated(_, flag))
          _ <-
            Journal.record(s, actor, None, "flag", definition.key.value, "created", definition.asJson, events)
        yield ()

    def archive(key: FlagKey, actor: Actor): F[Boolean] = pool.use: s =>
      s.transaction.surround:
        s.execute(Sql.archive)(key).flatMap:
          case Completion.Update(0) => false.pure[F]
          case _                    =>
            for
              envs <- s.execute(Sql.selectEnvironments)
              events = envs.map(ChangeEvent.FlagArchived(_, key))
              _ <- Journal.record(s, actor, None, "flag", key.value, "archived", key.asJson, events)
            yield true

    def list(env: EnvironmentKey): F[List[FeatureFlag]] = pool.use(_.execute(Sql.selectFlags)(env))

    def get(env: EnvironmentKey, key: FlagKey): F[Option[FeatureFlag]] =
      pool.use(_.option(Sql.selectFlag)((env, key)))

    def updateConfig(
        env: EnvironmentKey,
        key: FlagKey,
        expectedVersion: Long,
        config: FlagConfig,
        actor: Actor
    ): F[FeatureFlag] = pool.use: s =>
      s.transaction.surround:
        for
          _ <- s
            .option(Sql.updateConfig)((config, env, key, expectedVersion))
            .flatMap(
              _.liftTo[F](
                AdminError.Conflict(
                  s"flag '$key' in '$env' was modified concurrently: expected version $expectedVersion"
                )
              )
            )
          flag <- s.unique(Sql.selectFlag)((env, key))
          _    <- Journal.record(
            s,
            actor,
            Some(env),
            "flag_config",
            key.value,
            "updated",
            flag.asJson,
            List(ChangeEvent.FlagUpdated(env, flag))
          )
        yield flag
