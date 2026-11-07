package flagforge.admin.service

import cats.MonadThrow
import cats.data.ValidatedNec
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import flagforge.admin.store.{EnvironmentRepository, FlagRepository, SegmentRepository}
import flagforge.admin.{Actor, AdminError}
import flagforge.domain.*
import flagforge.eval.{Evaluator, FlagValidator}

/** Business rules of the control plane: validation, existence checks and composition of repositories. */
trait AdminService[F[_]]:
  def listEnvironments: F[List[Environment]]
  def createEnvironment(environment: Environment, actor: Actor): F[Environment]

  def listFlags(includeArchived: Boolean): F[List[FlagDefinition]]
  def getFlag(key: FlagKey): F[FlagDefinition]
  def createFlag(request: CreateFlagRequest, actor: Actor): F[FlagDefinition]
  def archiveFlag(key: FlagKey, actor: Actor): F[Unit]

  def listFlagConfigs(env: EnvironmentKey): F[List[FeatureFlag]]
  def getFlagConfig(env: EnvironmentKey, key: FlagKey): F[FeatureFlag]
  def updateFlagConfig(env: EnvironmentKey, key: FlagKey, request: UpdateFlagConfigRequest, actor: Actor): F[FeatureFlag]

  /** Kill switch: flips `enabled` without touching targeting. */
  def toggleFlag(env: EnvironmentKey, key: FlagKey, enabled: Boolean, actor: Actor): F[FeatureFlag]

  def listSegments(env: EnvironmentKey): F[List[Segment]]
  def getSegment(env: EnvironmentKey, key: SegmentKey): F[Segment]
  def saveSegment(env: EnvironmentKey, key: SegmentKey, request: SaveSegmentRequest, actor: Actor): F[Segment]

  def ruleset(env: EnvironmentKey): F[Ruleset]

  /** Server-side evaluation of every flag for a context. Handy for debugging targeting. */
  def evaluate(env: EnvironmentKey, context: EvaluationContext): F[List[Evaluation]]

object AdminService:

  def make[F[_]: MonadThrow: UUIDGen](
      environments: EnvironmentRepository[F],
      flags: FlagRepository[F],
      segments: SegmentRepository[F]
  ): AdminService[F] = new:

    private def notFound[A](what: String): F[A] = AdminError.NotFound(s"$what not found").raiseError

    private def validOrRaise[A](what: String)(result: ValidatedNec[String, A]): F[A] =
      result.fold(errors => AdminError.Invalid(s"invalid $what", errors.toList).raiseError, _.pure)

    private def requireEnvironment(env: EnvironmentKey): F[Unit] =
      environments.get(env).flatMap(_.fold(notFound(s"environment '$env'"))(_ => ().pure))

    def listEnvironments: F[List[Environment]] = environments.list

    def createEnvironment(environment: Environment, actor: Actor): F[Environment] =
      environments.create(environment, actor).as(environment)

    def listFlags(includeArchived: Boolean): F[List[FlagDefinition]] = flags.listDefinitions(includeArchived)

    def getFlag(key: FlagKey): F[FlagDefinition] =
      flags.getDefinition(key).flatMap(_.fold(notFound(s"flag '$key'"))(_.pure))

    def createFlag(request: CreateFlagRequest, actor: Actor): F[FlagDefinition] =
      for
        _       <- validOrRaise("flag")(FlagValidator.validateVariations(request.kind, request.variations))
        initial <- FlagConfig.initial(request.variations).liftTo[F](AdminError.Invalid("invalid flag", List("no variations")))
        salt    <- UUIDGen[F].randomUUID.map(_.toString.take(8))
        definition = FlagDefinition(
          request.key,
          request.name,
          request.kind,
          request.variations,
          salt,
          request.description,
          request.tags
        )
        _ <- flags.create(definition, initial, actor)
      yield definition

    def archiveFlag(key: FlagKey, actor: Actor): F[Unit] =
      flags.archive(key, actor).flatMap(archived => notFound(s"flag '$key'").unlessA(archived))

    def listFlagConfigs(env: EnvironmentKey): F[List[FeatureFlag]] =
      requireEnvironment(env) >> flags.list(env)

    def getFlagConfig(env: EnvironmentKey, key: FlagKey): F[FeatureFlag] =
      requireEnvironment(env) >> flags.get(env, key).flatMap(_.fold(notFound(s"flag '$key' in '$env'"))(_.pure))

    def updateFlagConfig(
        env: EnvironmentKey,
        key: FlagKey,
        request: UpdateFlagConfigRequest,
        actor: Actor
    ): F[FeatureFlag] =
      for
        current     <- getFlagConfig(env, key)
        all         <- flags.list(env)
        segmentKeys <- segments.list(env).map(_.map(_.key).toSet)
        candidate = current.copy(config = request.config)
        _       <- validOrRaise("flag configuration")(FlagValidator.validateConfig(candidate, all.map(f => f.key -> f).toMap, segmentKeys))
        updated <- flags.updateConfig(env, key, request.version, request.config, actor)
      yield updated

    def toggleFlag(env: EnvironmentKey, key: FlagKey, enabled: Boolean, actor: Actor): F[FeatureFlag] =
      getFlagConfig(env, key).flatMap: current =>
        if current.config.enabled == enabled then current.pure
        else flags.updateConfig(env, key, current.version, current.config.copy(enabled = enabled), actor)

    def listSegments(env: EnvironmentKey): F[List[Segment]] =
      requireEnvironment(env) >> segments.list(env)

    def getSegment(env: EnvironmentKey, key: SegmentKey): F[Segment] =
      requireEnvironment(env) >> segments.get(env, key).flatMap(_.fold(notFound(s"segment '$key' in '$env'"))(_.pure))

    def saveSegment(env: EnvironmentKey, key: SegmentKey, request: SaveSegmentRequest, actor: Actor): F[Segment] =
      for
        _       <- requireEnvironment(env)
        segment <- validOrRaise("segment")(FlagValidator.validateSegment(request.toSegment(key)))
        saved   <- segments.save(env, segment, actor)
      yield saved

    def ruleset(env: EnvironmentKey): F[Ruleset] =
      requireEnvironment(env) >> (flags.list(env), segments.list(env)).mapN: (fs, ss) =>
        Ruleset(env, fs.map(f => f.key -> f).toMap, ss.map(s => s.key -> s).toMap)

    def evaluate(env: EnvironmentKey, context: EvaluationContext): F[List[Evaluation]] =
      ruleset(env).map(rs => Evaluator.evaluateAll(rs, context).values.toList.sortBy(_.flag))
