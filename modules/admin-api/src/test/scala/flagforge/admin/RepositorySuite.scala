package flagforge.admin

import cats.effect.{IO, Resource}
import flagforge.admin.store.{EnvironmentRepository, FlagRepository, SegmentRepository}
import flagforge.domain.*
import io.circe.Json
import org.typelevel.otel4s.trace.Tracer.Implicits.noop
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

class RepositorySuite extends PostgresSuite:

  private val actor = Actor("test")

  private def env(key: String)      = Environment(EnvironmentKey.applyUnsafe(key), key.capitalize)
  private def vk(key: String)       = VariationKey.applyUnsafe(key)
  private def boolFlag(key: String) = FlagDefinition(
    FlagKey.applyUnsafe(key),
    s"Flag $key",
    FlagKind.Boolean,
    List(Variation(vk("on"), Json.True), Variation(vk("off"), Json.False)),
    salt = "salt"
  )
  private def initial(flag: FlagDefinition) = FlagConfig.initial(flag.variations).get

  private def outboxCount(pool: Resource[IO, Session[IO]], keyPattern: String): IO[Long] =
    pool.use(_.unique(sql"SELECT count(*) FROM outbox WHERE key LIKE $text".query(int8))(keyPattern))

  private case class Repos(
      envs: EnvironmentRepository[IO],
      flags: FlagRepository[IO],
      segments: SegmentRepository[IO]
  )

  private def withRepos[A](f: (Repos, Resource[IO, Session[IO]]) => IO[A]): IO[A] =
    withDatabase: pool =>
      f(
        Repos(
          EnvironmentRepository.postgres(pool),
          FlagRepository.postgres(pool),
          SegmentRepository.postgres(pool)
        ),
        pool
      )

  test("a new flag gets a disabled config in every environment, plus outbox events"):
    withRepos: (repos, pool) =>
      val flag = boolFlag("repo-new-flag")
      for
        _      <- repos.envs.create(env("repo-a"), actor)
        _      <- repos.envs.create(env("repo-b"), actor)
        _      <- repos.flags.create(flag, initial(flag), actor)
        inA    <- repos.flags.get(EnvironmentKey.applyUnsafe("repo-a"), flag.key)
        inB    <- repos.flags.get(EnvironmentKey.applyUnsafe("repo-b"), flag.key)
        events <- outboxCount(pool, "repo-_/flag/repo-new-flag")
      yield
        assertEquals(
          inA.map(f => (f.version, f.config.enabled, f.config.offVariation)),
          Some((1L, false, vk("off")))
        )
        assertEquals(inB.map(_.version), Some(1L))
        assertEquals(events, 2L)

  test("an environment created later gets configs for existing flags"):
    withRepos: (repos, _) =>
      val flag = boolFlag("repo-early-flag")
      for
        _   <- repos.flags.create(flag, initial(flag), actor)
        _   <- repos.envs.create(env("repo-late"), actor)
        got <- repos.flags.get(EnvironmentKey.applyUnsafe("repo-late"), flag.key)
      yield assertEquals(got.map(_.key), Some(flag.key))

  test("duplicate keys are conflicts"):
    withRepos: (repos, _) =>
      val flag    = boolFlag("repo-dup")
      val attempt = repos.flags.create(flag, initial(flag), actor) >>
        repos.flags.create(flag, initial(flag), actor)
      interceptIO[AdminError.Conflict](attempt) >>
        interceptIO[AdminError.Conflict](repos.envs.create(env("repo-dup-env"), actor).replicateA_(2))

  test("config updates use optimistic locking"):
    withRepos: (repos, pool) =>
      val flag    = boolFlag("repo-locking")
      val envKey  = EnvironmentKey.applyUnsafe("repo-locking-env")
      val enabled = initial(flag).copy(enabled = true)
      for
        _       <- repos.envs.create(env("repo-locking-env"), actor)
        _       <- repos.flags.create(flag, initial(flag), actor)
        updated <- repos.flags.updateConfig(envKey, flag.key, expectedVersion = 1, enabled, actor)
        _ <- interceptIO[AdminError.Conflict](repos.flags.updateConfig(envKey, flag.key, 1, enabled, actor))
        events <- outboxCount(pool, "repo-locking-env/flag/repo-locking")
      yield
        assertEquals(updated.version, 2L)
        assert(updated.config.enabled)
        assertEquals(events, 2L, "one event for creation, one for the successful update")

  test("archived flags disappear from environments"):
    withRepos: (repos, _) =>
      val flag   = boolFlag("repo-archive")
      val envKey = EnvironmentKey.applyUnsafe("repo-archive-env")
      for
        _      <- repos.envs.create(env("repo-archive-env"), actor)
        _      <- repos.flags.create(flag, initial(flag), actor)
        first  <- repos.flags.archive(flag.key, actor)
        second <- repos.flags.archive(flag.key, actor)
        got    <- repos.flags.get(envKey, flag.key)
        defs   <- repos.flags.listDefinitions(includeArchived = true)
      yield
        assertEquals((first, second), (true, false))
        assertEquals(got, None)
        assert(defs.find(_.key == flag.key).exists(_.archived))

  test("segments are created with version 1 and updated optimistically"):
    withRepos: (repos, _) =>
      val envKey  = EnvironmentKey.applyUnsafe("repo-segments")
      val segment = Segment(SegmentKey.applyUnsafe("beta"), "Beta", included = Set("alice"))
      for
        _       <- repos.envs.create(env("repo-segments"), actor)
        created <- repos.segments.save(envKey, segment, actor)
        _       <- interceptIO[AdminError.Conflict](repos.segments.save(envKey, segment, actor))
        updated <- repos.segments.save(envKey, created.copy(included = Set("alice", "bob")), actor)
        _       <- interceptIO[AdminError.Conflict](repos.segments.save(envKey, created, actor))
        stored  <- repos.segments.get(envKey, segment.key)
      yield
        assertEquals(created.version, 1L)
        assertEquals(updated.version, 2L)
        assertEquals(stored, Some(updated))
