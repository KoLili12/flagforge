package flagforge.delivery

import cats.effect.Async
import cats.syntax.all.*
import ciris.*
import com.comcast.ip4s.{Host, Port}
import flagforge.messaging.KafkaConfig

import scala.concurrent.duration.*

final case class DeliveryConfig(
    host: Host,
    port: Port,
    kafka: KafkaConfig,
    redisUri: Option[String],
    heartbeat: FiniteDuration = 15.seconds,
    snapshotDebounce: FiniteDuration = 1.second,
    maxRecentEvents: Int = DeliveryState.DefaultMaxRecent
)

object DeliveryConfig:

  private given ConfigDecoder[String, Host] = ConfigDecoder[String].mapOption("Host")(Host.fromString)
  private given ConfigDecoder[Int, Port]    = ConfigDecoder[Int].mapOption("Port")(Port.fromInt)

  def load[F[_]: Async]: F[DeliveryConfig] =
    (
      env("HTTP_HOST").as[Host].default(Host.fromString("0.0.0.0").get),
      env("HTTP_PORT").as[Int].as[Port].default(Port.fromInt(8081).get),
      env("KAFKA_BOOTSTRAP_SERVERS").default("localhost:9092").map(KafkaConfig(_)),
      env("REDIS_URI").option,
      env("SSE_HEARTBEAT").as[FiniteDuration].default(15.seconds),
      env("SNAPSHOT_DEBOUNCE").as[FiniteDuration].default(1.second),
      env("MAX_RECENT_EVENTS").as[Int].default(DeliveryState.DefaultMaxRecent)
    ).parMapN(DeliveryConfig.apply).load[F]
