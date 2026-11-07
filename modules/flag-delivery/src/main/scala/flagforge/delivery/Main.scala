package flagforge.delivery

import cats.effect.syntax.all.*
import cats.effect.{Async, IO, IOApp, Ref, Resource}
import cats.syntax.all.*
import flagforge.messaging.ChangeFeed
import flagforge.telemetry.Telemetry
import fs2.concurrent.{Signal, SignallingRef}
import fs2.io.net.Network
import org.http4s.HttpApp
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory

import scala.concurrent.duration.*

object DeliveryApp:

  final case class Running[F[_]](app: HttpApp[F], state: Signal[F, DeliveryState], ready: Signal[F, Boolean])

  /** Consumer, snapshotting and HTTP app, without binding a port. */
  def resource[F[_]: Async: LoggerFactory](
      config: DeliveryConfig,
      snapshots: SnapshotStore[F],
      telemetry: Telemetry[F]
  ): Resource[F, Running[F]] =
    val logger = LoggerFactory[F].getLogger

    /** Restarts `fa` after failures, e.g. while Kafka is unavailable. */
    def supervised(name: String)(fa: F[Unit]): F[Unit] = (fa >> logger.warn(s"$name stopped, restarting"))
      .handleErrorWith(e => logger.error(e)(s"$name failed, restarting in 5s") >> Async[F].sleep(5.seconds))
      .foreverM

    val initialState: F[DeliveryState] =
      snapshots.load.attempt.flatMap:
        case Right(Some(snapshot)) =>
          logger.info(s"Warm start from snapshot at offset ${snapshot.offset}")
            .as(DeliveryState.fromSnapshot(snapshot, config.maxRecentEvents))
        case Right(None) => DeliveryState.empty(config.maxRecentEvents).pure[F]
        case Left(e)     =>
          logger.warn(e)(
            "Could not load snapshot, cold start"
          ).as(DeliveryState.empty(config.maxRecentEvents))

    def snapshotter(state: Signal[F, DeliveryState]): F[Unit] =
      state.discrete
        .debounce(config.snapshotDebounce)
        .filter(_.offset >= 0)
        .changesBy(_.offset)
        .evalMap(s =>
          snapshots.save(s.snapshot).handleErrorWith(e => logger.warn(e)("Could not save snapshot"))
        )
        .compile
        .drain

    for
      _           <- Resource.eval(ChangeFeed.ensureTopic[F](config.kafka))
      state       <- Resource.eval(initialState.flatMap(SignallingRef.of[F, DeliveryState]))
      ready       <- Resource.eval(SignallingRef.of[F, Boolean](false))
      connections <- Resource.eval(Ref.of[F, Int](0))
      _           <-
        supervised("Change feed consumer")(FeedConsumer(config.kafka, state, ready, telemetry).run).background
      _ <- telemetry.meter
        .observableUpDownCounter[Long]("flagforge.delivery.sse.connections")
        .withDescription("Open SSE connections")
        .createWithCallback(cb => connections.get.flatMap(n => cb.record(n.toLong)))
      _ <- telemetry.meter
        .observableGauge[Long]("flagforge.delivery.offset")
        .withDescription("Last change feed offset applied")
        .createWithCallback(cb => state.get.flatMap(s => cb.record(s.offset)))
      _ <- supervised("Snapshotter")(snapshotter(state)).background
    yield Running(SdkRoutes(state, ready, connections, config.heartbeat).routes.orNotFound, state, ready)

  def server[F[_]: Async: Network: LoggerFactory](
      config: DeliveryConfig,
      snapshots: SnapshotStore[F],
      telemetry: Telemetry[F]
  ): Resource[F, Server] =
    for
      running <- resource[F](config, snapshots, telemetry)
      // An SSE stream lives for hours: a span per connection would be useless, the change spans tell the story.
      app <- Resource.eval(Telemetry.instrument(
        telemetry,
        _.segments.lastOption.exists(_.encoded == "stream")
      )(running.app))
      server <- EmberServerBuilder
        .default[F]
        .withHost(config.host)
        .withPort(config.port)
        .withHttpApp(app)
        .withMaxConnections(config.maxConnections)
        .withIdleTimeout(Duration.Inf) // SSE connections are long-lived; heartbeats keep proxies happy
        // Streams never complete on their own, so a graceful shutdown would always wait for the full timeout.
        // Clients reconnect (to another instance) with Last-Event-ID anyway.
        .withShutdownTimeout(2.seconds)
        .build
      _ <- Resource.eval(LoggerFactory[F].getLogger.info(s"Flag delivery on ${server.baseUri}"))
    yield server

object Main extends IOApp.Simple:
  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  def run: IO[Unit] =
    val app =
      for
        config    <- DeliveryConfig.load[IO].toResource
        snapshots <- config.redisUri.fold(
          Resource.pure[IO, SnapshotStore[IO]](SnapshotStore.noop[IO])
        )(RedisSnapshotStore.resource[IO])
        telemetry <- Telemetry.autoConfigured[IO]("flag-delivery")
        server    <- DeliveryApp.server[IO](config, snapshots, telemetry)
      yield server
    app.useForever
