package flagforge.domain

import io.circe.Json
import io.circe.derivation.ConfiguredCodec

/** The subject a flag is evaluated for: a user, an organisation, a device...
  *
  * `key` is the stable identifier used for targeting and bucketing. It is also available to clauses as the
  * built-in attribute `"key"`.
  */
final case class EvaluationContext(
    key: String,
    attributes: Map[String, Json] = Map.empty
) derives ConfiguredCodec:

  def attribute(name: String): Option[Json] =
    if name == EvaluationContext.KeyAttribute then Some(Json.fromString(key))
    else attributes.get(name)

  def withAttribute(name: String, value: Json): EvaluationContext =
    copy(attributes = attributes.updated(name, value))

object EvaluationContext:
  val KeyAttribute = "key"
