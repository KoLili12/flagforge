package flagforge.admin.relay

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream
import flagforge.telemetry.Telemetry
import fs2.kafka.{Header, Headers, KafkaProducer, ProducerRecord}
import io.circe.Json
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{BucketBoundaries, Counter, Histogram}
import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.data.{Arr, Identifier}
import skunk.implicits.*

import scala.concurrent.duration.*

final case class RelayConfig(
    enabled: Boolean,
    batchSize: Int,
    pollInterval: FiniteDuration,
    retryDelay: FiniteDuration = 5.seconds
)

/** Moves committed outbox rows to Kafka.
  *
  *   - Wakes up on `LISTEN outbox_inserted` (sub-second latency) and on a poll timer (safety net).
  *   - A transaction-scoped advisory lock makes sure only one relay publishes at a time, which preserves the
  *     commit order of events even with several admin-api instances running.
  *   - Rows are marked as published only after Kafka acknowledged them, in the same transaction that locked
  *     them: at-least-once delivery. Duplicates are harmless because consumers apply events by version.
  */
final class OutboxRelay[F[_]: Async: LoggerFactory] private (
    pool: Resource[F, Session[F]],
    producer: KafkaProducer[F, String, String],
    config: RelayConfig,
    telemetry: Telemetry[F],
    published: Counter[F, Long],
    lag: Histogram[F, Double]
):
  import OutboxRelay.*

  private val logger = LoggerFactory[F].getLogger

  /** Publishes at most one batch; returns the number of published rows. */
  def publishBatch: F[Int] = pool.use: s =>
    s.transaction.surround:
      s.unique(Sql.tryLock)(LockId).flatMap:
        case false => 0.pure[F]
        case true  =>
          s.execute(Sql.fetch)(config.batchSize).flatMap: rows =>
            if rows.isEmpty then 0.pure[F]
            else
              val records = fs2.Chunk.from(rows.map(r =>
                ProducerRecord(r.topic, r.key, r.payload.noSpaces).withHeaders(r.kafkaHeaders)
              ))
              telemetry.tracer
                .span("outbox.publish", Attribute("messaging.batch.message_count", rows.size.toLong))
                .surround(producer.produce(records).flatten) *>
                s.execute(Sql.markPublished)(Arr(rows.map(_.id)*)) *>
                recordMetrics(rows).as(rows.size)

  private def recordMetrics(rows: List[Row]): F[Unit] =
    Async[F].realTime.flatMap: now =>
      published.add(rows.size.toLong) *>
        rows.traverse_(r => lag.record((now.toMillis - r.createdAtMillis) / 1000.0))

  /** Publishes until the outbox is drained. */
  def publishAll: F[Int] =
    publishBatch.flatMap(n => if n == config.batchSize then publishAll.map(_ + n) else n.pure[F])

  private def wakeups: Stream[F, Unit] =
    val notifications = Stream.resource(pool).flatMap(_.channel(Channel).listen(maxQueued = 64)).void
    val ticks         = Stream.awakeEvery[F](config.pollInterval).void
    Stream.unit ++ notifications.merge(ticks)

  /** Runs forever, restarting after failures (Kafka or Postgres unavailable). */
  def run: F[Unit] =
    wakeups
      .evalMap(_ => publishAll)
      .evalMap(n => logger.debug(s"Published $n change events").whenA(n > 0))
      .compile
      .drain
      .handleErrorWith: e =>
        logger.warn(e)(s"Outbox relay failed, restarting in ${config.retryDelay}") >>
          Async[F].sleep(config.retryDelay) >> run

object OutboxRelay:

  /** Arbitrary constant identifying the relay's advisory lock. */
  val LockId: Long = 0x0f1a_6f06L

  val Channel: Identifier = id"outbox_inserted"

  /** Header with the commit time of the change (epoch millis): consumers use it to measure propagation
    * latency.
    */
  val CreatedAtHeader = "flagforge-created-at"

  final case class Row(
      id: Long,
      topic: String,
      key: String,
      payload: Json,
      headers: Map[String, String],
      createdAtMillis: Long
  ):
    /** Trace context of the request that made the change, plus its commit time. */
    def kafkaHeaders: Headers =
      Headers.fromIterable(
        (headers + (CreatedAtHeader -> createdAtMillis.toString)).map((k, v) => Header(k, v))
      )

  def make[F[_]: Async: LoggerFactory](
      pool: Resource[F, Session[F]],
      producer: KafkaProducer[F, String, String],
      config: RelayConfig,
      telemetry: Telemetry[F]
  ): F[OutboxRelay[F]] =
    for
      published <- telemetry.meter
        .counter[Long]("flagforge.outbox.published")
        .withDescription("Change events published from the outbox to Kafka")
        .create
      lag <- telemetry.meter
        .histogram[Double]("flagforge.outbox.lag")
        .withUnit("s")
        .withDescription("Time from commit to publication in Kafka")
        .withExplicitBucketBoundaries(BucketBoundaries(
          0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
        ))
        .create
    yield new OutboxRelay(pool, producer, config, telemetry, published, lag)

  private object Sql:
    val tryLock: Query[Long, Boolean] = sql"SELECT pg_try_advisory_xact_lock($int8)".query(bool)

    val fetch: Query[Int, Row] =
      sql"""SELECT id, topic, key, payload, headers, (extract(epoch FROM created_at) * 1000)::int8
            FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $int4"""
        .query(int8 *: text *: text *: jsonb[Json] *: jsonb[Map[String, String]] *: int8)
        .to[Row]

    val markPublished: Command[Arr[Long]] =
      sql"UPDATE outbox SET published_at = now() WHERE id = ANY($_int8)".command
