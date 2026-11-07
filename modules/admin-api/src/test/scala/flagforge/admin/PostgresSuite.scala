package flagforge.admin

import cats.effect.{IO, Resource}
import ciris.Secret
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.TestContainerForAll
import flagforge.admin.db.Database
import munit.CatsEffectSuite
import org.testcontainers.utility.DockerImageName
import skunk.Session

/** One throw-away Postgres per suite. Tests share it, so each test uses its own keys. */
trait PostgresSuite extends CatsEffectSuite with TestContainerForAll:

  override val containerDef: PostgreSQLContainer.Def =
    PostgreSQLContainer.Def(DockerImageName.parse("postgres:17-alpine"))

  def dbConfig(pg: PostgreSQLContainer): DbConfig =
    DbConfig(pg.host, pg.mappedPort(5432), pg.username, Secret(pg.password), pg.databaseName, poolSize = 4)

  /** Migrated database and a session pool. Migrations are idempotent, so running them per test is fine. */
  def withDatabase[A](f: Resource[IO, Session[IO]] => IO[A]): IO[A] =
    withContainers: pg =>
      val config = dbConfig(pg)
      Database.migrate[IO](config) >> Database.pool[IO](config).use(f)
