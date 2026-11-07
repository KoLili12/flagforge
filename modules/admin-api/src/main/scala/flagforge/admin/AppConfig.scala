package flagforge.admin

import cats.effect.Async
import cats.syntax.all.*
import ciris.*
import com.comcast.ip4s.{Host, Port}
import flagforge.admin.relay.RelayConfig
import flagforge.messaging.KafkaConfig

import scala.concurrent.duration.*

final case class HttpConfig(host: Host, port: Port)

final case class DbConfig(
    host: String,
    port: Int,
    user: String,
    password: Secret[String],
    database: String,
    poolSize: Int
):
  def jdbcUrl: String = s"jdbc:postgresql://$host:$port/$database"

final case class AppConfig(http: HttpConfig, db: DbConfig, kafka: KafkaConfig, relay: RelayConfig)

object AppConfig:

  private given ConfigDecoder[String, Host] = ConfigDecoder[String].mapOption("Host")(Host.fromString)
  private given ConfigDecoder[Int, Port]    = ConfigDecoder[Int].mapOption("Port")(Port.fromInt)

  private val http: ConfigValue[Effect, HttpConfig] =
    (
      env("HTTP_HOST").as[Host].default(Host.fromString("0.0.0.0").get),
      env("HTTP_PORT").as[Int].as[Port].default(Port.fromInt(8080).get)
    ).parMapN(HttpConfig.apply)

  private val db: ConfigValue[Effect, DbConfig] =
    (
      env("DB_HOST").default("localhost"),
      env("DB_PORT").as[Int].default(5432),
      env("DB_USER").default("flagforge"),
      env("DB_PASSWORD").default("flagforge").secret,
      env("DB_NAME").default("flagforge"),
      env("DB_POOL_SIZE").as[Int].default(10)
    ).parMapN(DbConfig.apply)

  private val kafka: ConfigValue[Effect, KafkaConfig] =
    env("KAFKA_BOOTSTRAP_SERVERS").default("localhost:9092").map(KafkaConfig(_))

  private val relay: ConfigValue[Effect, RelayConfig] =
    (
      env("OUTBOX_RELAY_ENABLED").as[Boolean].default(true),
      env("OUTBOX_BATCH_SIZE").as[Int].default(500),
      env("OUTBOX_POLL_INTERVAL").as[FiniteDuration].default(2.seconds)
    ).parMapN(RelayConfig(_, _, _))

  def load[F[_]: Async]: F[AppConfig] = (http, db, kafka, relay).parMapN(AppConfig.apply).load[F]
