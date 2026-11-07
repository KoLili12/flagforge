import org.typelevel.sbt.tpolecat.DevMode

ThisBuild / scalaVersion               := "3.9.0"
ThisBuild / organization               := "io.flagforge"
ThisBuild / version                    := "0.1.0-SNAPSHOT"
ThisBuild / tpolecatDefaultOptionsMode := DevMode
// Docker images run on JRE 21; the build itself may use a newer JDK.
ThisBuild / scalacOptions ++= Seq("-release", "21")
ThisBuild / javacOptions ++= Seq("--release", "21")

lazy val testSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scalameta" %%% "munit"            % V.munit           % Test,
    "org.scalameta" %%% "munit-scalacheck" % V.munitScalacheck % Test
  )
)

/** Container image `flagforge/<service>:<version>` built with `sbt <project>/Docker/publishLocal`. */
def dockerSettings(port: Int) = Seq(
  dockerBaseImage      := "eclipse-temurin:21-jre",
  Docker / packageName := s"flagforge/${name.value.stripPrefix("flagforge-")}",
  dockerUpdateLatest   := true,
  dockerExposedPorts   := Seq(port),
  Docker / daemonUser  := "flagforge",
  dockerEnvVars        := Map("JAVA_OPTS" -> "-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"),
  dockerLabels         := Map("org.opencontainers.image.source" -> "https://github.com/flagforge/flagforge"),
  // Services are not libraries: skip scaladoc when packaging.
  Compile / packageDoc / mappings := Seq.empty
)

/** Settings for runnable JVM services. */
lazy val serviceSettings = Seq(
  run / fork  := true,
  Test / fork := true,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect"       % V.catsEffect,
    "is.cir"        %% "ciris"             % V.ciris,
    "org.typelevel" %% "log4cats-slf4j"    % V.log4cats,
    "ch.qos.logback" % "logback-classic"   % V.logback         % Runtime,
    "org.typelevel" %% "munit-cats-effect" % V.munitCatsEffect % Test,
    "ch.qos.logback" % "logback-classic"   % V.logback         % Test
  )
)

lazy val root = project
  .in(file("."))
  .aggregate(
    domain.jvm,
    domain.js,
    evalCore.jvm,
    evalCore.js,
    statsCore.jvm,
    statsCore.js,
    messaging,
    telemetry,
    adminApi,
    flagDelivery,
    eventIngest,
    statsEngine,
    sdk,
    simulator,
    loadTest,
    itTests
  )
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

/** Statistics for experiment analysis: z/t-tests, SRM, Bonferroni. Dependency-free maths, JVM + JS. */
lazy val statsCore = crossProject(JVMPlatform, JSPlatform)
  .crossType(CrossType.Pure)
  .in(file("modules/stats-core"))
  .settings(testSettings)
  .settings(
    name                               := "flagforge-stats-core",
    libraryDependencies += "io.circe" %%% "circe-core" % V.circe
  )

/** Kafka plumbing for the change feed shared by the producer (admin-api) and consumers (flag-delivery). */
lazy val messaging = project
  .in(file("modules/messaging"))
  .dependsOn(domain.jvm)
  .settings(testSettings)
  .settings(
    name := "flagforge-messaging",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka" % V.fs2Kafka
    )
  )

/** OpenTelemetry wiring shared by the services: SDK from OTEL_* env vars, HTTP tracing and metrics. */
lazy val telemetry = project
  .in(file("modules/telemetry"))
  .settings(testSettings)
  .settings(
    name := "flagforge-telemetry",
    libraryDependencies ++= Seq(
      "org.typelevel"   %% "otel4s-oteljava"                       % V.otel4s,
      "org.http4s"      %% "http4s-server"                         % V.http4s,
      "org.http4s"      %% "http4s-otel4s-middleware-trace-server" % V.otel4sHttp4s,
      "org.http4s"      %% "http4s-otel4s-middleware-metrics"      % V.otel4sHttp4s,
      "io.opentelemetry" % "opentelemetry-exporter-otlp"           % V.openTelemetry
    )
  )

/** Control plane: CRUD for environments, flags and segments, backed by Postgres; relays the outbox to Kafka.
  */
lazy val adminApi = project
  .in(file("modules/admin-api"))
  .dependsOn(evalCore.jvm, messaging, telemetry)
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(testSettings, serviceSettings, dockerSettings(8080))
  .settings(
    name                := "flagforge-admin-api",
    Compile / mainClass := Some("flagforge.admin.Main"),
    libraryDependencies ++= Seq(
      "org.http4s"                  %% "http4s-ember-server"             % V.http4s,
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server"             % V.tapir,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe"                % V.tapir,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle"         % V.tapir,
      "org.tpolecat"                %% "skunk-core"                      % V.skunk,
      "org.tpolecat"                %% "skunk-circe"                     % V.skunk,
      "org.flywaydb"                 % "flyway-core"                     % V.flyway,
      "org.flywaydb"                 % "flyway-database-postgresql"      % V.flyway,
      "org.postgresql"               % "postgresql"                      % V.postgresql,
      "io.circe"                    %% "circe-literal"                   % V.circe          % Test,
      "com.dimafeng"                %% "testcontainers-scala-munit"      % V.testcontainers % Test,
      "com.dimafeng"                %% "testcontainers-scala-postgresql" % V.testcontainers % Test
    )
  )

/** Data plane: consumes the change feed and streams rulesets to SDKs over Server-Sent Events. */
lazy val flagDelivery = project
  .in(file("modules/flag-delivery"))
  .dependsOn(evalCore.jvm, messaging, telemetry)
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(testSettings, serviceSettings, dockerSettings(8081))
  .settings(
    name                := "flagforge-flag-delivery",
    Compile / mainClass := Some("flagforge.delivery.Main"),
    libraryDependencies ++= Seq(
      "org.http4s"     %% "http4s-ember-server" % V.http4s,
      "org.http4s"     %% "http4s-dsl"          % V.http4s,
      "org.http4s"     %% "http4s-circe"        % V.http4s,
      "dev.profunktor" %% "redis4cats-effects"  % V.redis4cats
    )
  )

/** Receives exposure and metric events from SDKs: rate limiting and deduplication in Redis, then Kafka. */
lazy val eventIngest = project
  .in(file("modules/event-ingest"))
  .dependsOn(messaging, telemetry)
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(testSettings, serviceSettings, dockerSettings(8083))
  .settings(
    name                := "flagforge-event-ingest",
    Compile / mainClass := Some("flagforge.ingest.Main"),
    libraryDependencies ++= Seq(
      "org.http4s"     %% "http4s-ember-server"        % V.http4s,
      "org.http4s"     %% "http4s-dsl"                 % V.http4s,
      "org.http4s"     %% "http4s-circe"               % V.http4s,
      "dev.profunktor" %% "redis4cats-effects"         % V.redis4cats,
      "com.dimafeng"   %% "testcontainers-scala-munit" % V.testcontainers % Test
    )
  )

/** Consumes exposures and metric events into Postgres and serves experiment reports. */
lazy val statsEngine = project
  .in(file("modules/stats-engine"))
  .dependsOn(statsCore.jvm, messaging, telemetry)
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(testSettings, serviceSettings, dockerSettings(8082))
  .settings(
    name                := "flagforge-stats-engine",
    Compile / mainClass := Some("flagforge.statsengine.Main"),
    libraryDependencies ++= Seq(
      "org.http4s"    %% "http4s-ember-server"             % V.http4s,
      "org.http4s"    %% "http4s-dsl"                      % V.http4s,
      "org.http4s"    %% "http4s-circe"                    % V.http4s,
      "org.tpolecat"  %% "skunk-core"                      % V.skunk,
      "org.flywaydb"   % "flyway-core"                     % V.flyway,
      "org.flywaydb"   % "flyway-database-postgresql"      % V.flyway,
      "org.postgresql" % "postgresql"                      % V.postgresql,
      "com.dimafeng"  %% "testcontainers-scala-munit"      % V.testcontainers % Test,
      "com.dimafeng"  %% "testcontainers-scala-postgresql" % V.testcontainers % Test
    )
  )

/** Scala SDK: keeps a live ruleset over SSE and evaluates flags locally. */
lazy val sdk = project
  .in(file("modules/sdk"))
  .dependsOn(evalCore.jvm)
  .settings(testSettings)
  .settings(
    name := "flagforge-sdk",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect"         % V.catsEffect,
      "org.http4s"    %% "http4s-ember-client" % V.http4s,
      "org.typelevel" %% "log4cats-core"       % V.log4cats,
      "org.typelevel" %% "munit-cats-effect"   % V.munitCatsEffect % Test,
      "org.http4s"    %% "http4s-dsl"          % V.http4s          % Test,
      "org.http4s"    %% "http4s-ember-server" % V.http4s          % Test,
      "ch.qos.logback" % "logback-classic"     % V.logback         % Test
    )
  )

/** Demo traffic: sets up an experiment through the admin API and simulates users through the real SDK. */
lazy val simulator = project
  .in(file("modules/simulator"))
  .dependsOn(sdk)
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(serviceSettings, dockerSettings(9000))
  .settings(
    name                := "flagforge-simulator",
    Compile / mainClass := Some("flagforge.simulator.Main"),
    dockerExposedPorts  := Nil,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-client" % V.http4s,
      "org.http4s" %% "http4s-circe"        % V.http4s
    )
  )

/** Load tests: SSE fan-out latency and ingest throughput against a running system. */
lazy val loadTest = project
  .in(file("modules/load-test"))
  .dependsOn(domain.jvm)
  .settings(serviceSettings)
  .settings(
    name                := "flagforge-load-test",
    publish / skip      := true,
    Compile / mainClass := Some("flagforge.loadtest.Main"),
    run / javaOptions ++= Seq("-Xmx2g"),
    run / connectInput := true,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-ember-client" % V.http4s,
      "org.http4s" %% "http4s-circe"        % V.http4s
    )
  )

/** End-to-end tests of the whole pipeline against real Postgres, Kafka and Redis. */
lazy val itTests = project
  .in(file("modules/it-tests"))
  .dependsOn(adminApi, flagDelivery, eventIngest, statsEngine, sdk)
  .settings(testSettings, serviceSettings)
  .settings(
    name           := "flagforge-it-tests",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.http4s"        %% "http4s-ember-client"             % V.http4s,
      "org.testcontainers" % "testcontainers-kafka"            % V.testcontainersJava % Test,
      "com.dimafeng"      %% "testcontainers-scala-postgresql" % V.testcontainers     % Test,
      "io.circe"          %% "circe-literal"                   % V.circe              % Test
    )
  )
