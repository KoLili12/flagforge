package flagforge.domain

import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.string.Match

/** Lowercase slug: starts with a letter or a digit, then letters, digits, `-`, `_` or `.`; 1..64 chars. */
type KeyFormat = Match["^[a-z0-9][a-z0-9_.-]{0,63}$"]

/** Shared instances for every slug-like identifier. Each companion gets its own opaque type `T`. */
sealed trait KeyType extends RefinedType[String, KeyFormat]:
  given Encoder[T]    = Encoder.encodeString.contramap(_.value)
  given Decoder[T]    = Decoder.decodeString.emap(s => either(s).left.map(_ => s"invalid key '$s'"))
  given KeyEncoder[T] = KeyEncoder.instance(_.value)
  given KeyDecoder[T] = KeyDecoder.instance(option)
  given Ordering[T]   = Ordering.by[T, String](_.value)

type EnvironmentKey = EnvironmentKey.T
object EnvironmentKey extends KeyType

type FlagKey = FlagKey.T
object FlagKey extends KeyType

type VariationKey = VariationKey.T
object VariationKey extends KeyType

type SegmentKey = SegmentKey.T
object SegmentKey extends KeyType

type RuleId = RuleId.T
object RuleId extends KeyType

type MetricKey = MetricKey.T
object MetricKey extends KeyType
