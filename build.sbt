import org.typelevel.sbt.tpolecat.DevMode

ThisBuild / scalaVersion               := "3.9.0"
ThisBuild / organization               := "io.flagforge"
ThisBuild / version                    := "0.1.0-SNAPSHOT"
ThisBuild / tpolecatDefaultOptionsMode := DevMode

lazy val testSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scalameta" %%% "munit"            % V.munit           % Test,
    "org.scalameta" %%% "munit-scalacheck" % V.munitScalacheck % Test
  )
)

lazy val root = project
  .in(file("."))
  .aggregate(domain.jvm, domain.js, evalCore.jvm, evalCore.js, adminApi)
  .settings(
    name           := "flagforge",
    publish / skip := true
  )

/** Pure domain model shared by the server, the SDK and (later) the Scala.js UI. */
lazy val domain = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("modules/domain"))
  .settings(testSettings)
  .settings(
    name := "flagforge-domain",
    libraryDependencies ++= Seq(
      "org.typelevel"      %%% "cats-core"    % V.cats,
      "io.circe"           %%% "circe-core"   % V.circe,
      "io.circe"           %%% "circe-parser" % V.circe,
      "io.github.iltotore" %%% "iron"         % V.iron
    )
  )

/** Deterministic, side-effect free flag evaluation. Runs identically on the JVM and in the browser. */
lazy val evalCore = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("modules/eval-core"))
  .dependsOn(domain)
  .settings(testSettings)
  .settings(name := "flagforge-eval-core")

/** Control plane: CRUD for environments, flags and segments, backed by Postgres. */
lazy val adminApi = project
  .in(file("modules/admin-api"))
  .dependsOn(evalCore.jvm)
  .settings(testSettings)
  .settings(
    name                := "flagforge-admin-api",
    run / fork          := true,
    Test / fork         := true,
    Compile / mainClass := Some("flagforge.admin.Main"),
    libraryDependencies ++= Seq(
      "org.typelevel"               %% "cats-effect"                     % V.catsEffect,
      "org.http4s"                  %% "http4s-ember-server"             % V.http4s,
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server"             % V.tapir,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe"                % V.tapir,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle"         % V.tapir,
      "org.tpolecat"                %% "skunk-core"                      % V.skunk,
      "org.tpolecat"                %% "skunk-circe"                     % V.skunk,
      "org.flywaydb"                 % "flyway-core"                     % V.flyway,
      "org.flywaydb"                 % "flyway-database-postgresql"      % V.flyway,
      "org.postgresql"               % "postgresql"                      % V.postgresql,
      "is.cir"                      %% "ciris"                           % V.ciris,
      "org.typelevel"               %% "log4cats-slf4j"                  % V.log4cats,
      "ch.qos.logback"               % "logback-classic"                 % V.logback         % Runtime,
      "org.typelevel"               %% "munit-cats-effect"               % V.munitCatsEffect % Test,
      "io.circe"                    %% "circe-literal"                   % V.circe           % Test,
      "com.dimafeng"                %% "testcontainers-scala-munit"      % V.testcontainers  % Test,
      "com.dimafeng"                %% "testcontainers-scala-postgresql" % V.testcontainers  % Test,
      "ch.qos.logback"               % "logback-classic"                 % V.logback         % Test
    )
  )
