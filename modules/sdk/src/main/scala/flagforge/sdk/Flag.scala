package flagforge.sdk

import flagforge.domain.*
import io.circe.{Decoder, Json}

/** A typed handle to a flag: its key, the value to use when the flag cannot be evaluated, and how to read the
  * served JSON value.
  *
  * {{{
  * val NewCheckout = Flag.bool("new-checkout", default = false)  // key checked at compile time
  * val Pricing     = Flag.json[PricingConfig]("pricing", default = PricingConfig.Default)
  * }}}
  */
final case class Flag[A](key: FlagKey, default: A, read: Json => Option[A])

object Flag:

  inline def bool(inline key: String, default: Boolean): Flag[Boolean] =
    Flag(FlagKey(key), default, _.asBoolean)

  inline def string(inline key: String, default: String): Flag[String] =
    Flag(FlagKey(key), default, _.asString)

  inline def int(inline key: String, default: Int): Flag[Int] =
    Flag(FlagKey(key), default, _.asNumber.flatMap(_.toInt))

  inline def double(inline key: String, default: Double): Flag[Double] =
    Flag(FlagKey(key), default, _.asNumber.map(_.toDouble))

  inline def json[A: Decoder](inline key: String, default: A): Flag[A] =
    Flag(FlagKey(key), default, _.as[A].toOption)

  /** For keys only known at runtime, e.g. read from configuration. */
  def dynamic[A](key: String, default: A)(read: Json => Option[A]): Either[String, Flag[A]] =
    FlagKey.either(key).map(Flag(_, default, read))

/** Typed evaluation result. On any problem `value` is the flag's default and `reason` says why. */
final case class FlagResult[A](value: A, variation: Option[VariationKey], reason: Reason):
  def isDefault: Boolean = variation.isEmpty

object LocalEvaluation:

  /** Evaluates against the latest known ruleset; never fails. */
  def evaluate[A](ruleset: Option[Ruleset], flag: Flag[A], context: EvaluationContext): FlagResult[A] =
    def fallback(kind: ErrorKind, message: String) =
      FlagResult(flag.default, None, Reason.Error(kind, message))

    ruleset match
      case None     => fallback(ErrorKind.ClientNotReady, "no ruleset received yet")
      case Some(rs) =>
        val evaluation = flagforge.eval.Evaluator.evaluate(rs, flag.key, context)
        evaluation.value match
          case None       => FlagResult(flag.default, None, evaluation.reason)
          case Some(json) =>
            flag.read(json) match
              case Some(value) => FlagResult(value, evaluation.variation, evaluation.reason)
              case None        => fallback(ErrorKind.WrongType, s"unexpected value ${json.noSpaces}")
