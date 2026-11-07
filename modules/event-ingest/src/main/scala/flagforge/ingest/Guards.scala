package flagforge.ingest

import cats.effect.{Async, Ref, Resource}
import cats.syntax.all.*
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.effect.Log
import dev.profunktor.redis4cats.effects.ScriptOutputType
import org.typelevel.log4cats.LoggerFactory

import scala.concurrent.duration.*

enum RateDecision:
  case Allowed
  case Limited(retryAfter: FiniteDuration)

final case class RateLimit(capacity: Int, refillPerSecond: Int)

/** Shared state that protects the pipeline: per-client rate limits and event deduplication. */
trait Guards[F[_]]:
  /** Takes `tokens` from the client's bucket, or tells when enough tokens will be available. */
  def acquire(clientKey: String, tokens: Int): F[RateDecision]

  /** Marks keys as seen; for each key, `true` if it was not seen within the TTL. */
  def firstSeen(keys: List[String]): F[List[Boolean]]

  /** Un-marks keys, e.g. when the events could not be published and the client will retry them. */
  def forget(keys: List[String]): F[Unit]

object Guards:

  /** Token bucket, refilled continuously. Atomic and uses the Redis clock, so any number of ingest instances
    * share one bucket per client without coordination or clock skew.
    */
  private val TokenBucket =
    """local capacity = tonumber(ARGV[1])
      |local rate = tonumber(ARGV[2])
      |local requested = tonumber(ARGV[3])
      |local time = redis.call('TIME')
      |local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
      |local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
      |local tokens = tonumber(state[1]) or capacity
      |local ts = tonumber(state[2]) or now
      |tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate / 1000)
      |local allowed = 0
      |local retry = 0
      |if tokens >= requested then
      |  tokens = tokens - requested
      |  allowed = 1
      |else
      |  retry = math.ceil((requested - tokens) * 1000 / rate)
      |end
      |redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
      |redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 1000 / rate) + 1000)
      |return {tostring(allowed), tostring(retry)}""".stripMargin

  /** `SET NX EX` for many keys in one round trip. */
  private val MarkSeen =
    """local result = {}
      |for i, key in ipairs(KEYS) do
      |  if redis.call('SET', key, '1', 'NX', 'EX', ARGV[1]) then result[i] = '1' else result[i] = '0' end
      |end
      |return result""".stripMargin

  def redis[F[_]: Async: LoggerFactory](
      uri: String,
      limit: RateLimit,
      dedupTtl: FiniteDuration
  ): Resource[F, Guards[F]] =
    val logger = LoggerFactory[F].getLoggerFromName("redis")
    given Log[F] = new Log[F]:
      def debug(msg: => String): F[Unit] = logger.debug(msg)
      def error(msg: => String): F[Unit] = logger.error(msg)
      def info(msg: => String): F[Unit]  = logger.info(msg)

    Redis[F]
      .utf8(uri)
      .map: redis =>
        new Guards[F]:
          def acquire(clientKey: String, tokens: Int): F[RateDecision] =
            redis
              .eval(
                TokenBucket,
                ScriptOutputType.Multi[String],
                List(s"ratelimit:$clientKey"),
                List(limit.capacity.toString, limit.refillPerSecond.toString, tokens.toString)
              )
              .map:
                case "1" :: _          => RateDecision.Allowed
                case _ :: retry :: Nil => RateDecision.Limited(retry.toLong.millis)
                case other             => throw IllegalStateException(s"unexpected script result $other")

          def firstSeen(keys: List[String]): F[List[Boolean]] =
            if keys.isEmpty then Nil.pure[F]
            else
              redis
                .eval(MarkSeen, ScriptOutputType.Multi[String], keys.map("seen:" + _), List(dedupTtl.toSeconds.toString))
                .map(_.map(_ == "1"))

          def forget(keys: List[String]): F[Unit] =
            keys.map("seen:" + _) match
              case head :: tail => redis.del(head, tail*).void
              case Nil          => ().pure[F]

  /** Single-process implementation for tests. */
  def inMemory[F[_]: Async](limit: RateLimit): F[Guards[F]] =
    (Ref.of[F, Map[String, Int]](Map.empty), Ref.of[F, Set[String]](Set.empty)).mapN: (buckets, seen) =>
      new Guards[F]:
        def acquire(clientKey: String, tokens: Int): F[RateDecision] =
          buckets.modify: m =>
            val available = m.getOrElse(clientKey, limit.capacity)
            if available >= tokens then (m.updated(clientKey, available - tokens), RateDecision.Allowed)
            else (m, RateDecision.Limited(1.second))

        def firstSeen(keys: List[String]): F[List[Boolean]] =
          seen.modify(s => keys.foldLeft((s, List.empty[Boolean])) { case ((acc, out), k) =>
            (acc + k, out :+ !acc.contains(k))
          })

        def forget(keys: List[String]): F[Unit] = seen.update(_ -- keys)
