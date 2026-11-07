package flagforge.admin.db

import flagforge.domain.*
import skunk.Codec
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*

/** skunk codecs for domain types. Column order matches the case class field order. */
object codecs:

  val environmentKey: Codec[EnvironmentKey] = text.eimap(EnvironmentKey.either)(_.value)
  val flagKey: Codec[FlagKey]               = text.eimap(FlagKey.either)(_.value)
  val segmentKey: Codec[SegmentKey]         = text.eimap(SegmentKey.either)(_.value)

  val flagKind: Codec[FlagKind] =
    text.eimap(s => FlagKind.values.find(_.toString.equalsIgnoreCase(s)).toRight(s"unknown flag kind '$s'"))(
      _.toString.toLowerCase
    )

  val environment: Codec[Environment] = (environmentKey *: text).to[Environment]

  /** key, name, kind, variations, salt, description, tags, archived */
  val flagDefinition: Codec[FlagDefinition] =
    (flagKey *: text *: flagKind *: jsonb[List[Variation]] *: text *: text.opt *: jsonb[List[String]] *: bool)
      .to[FlagDefinition]

  /** key, kind, variations, salt, version, config */
  val featureFlag: Codec[FeatureFlag] =
    (flagKey *: flagKind *: jsonb[List[Variation]] *: text *: int8 *: jsonb[FlagConfig]).to[FeatureFlag]

  /** The document column is authoritative for everything except the version. */
  val segment: Codec[Segment] =
    (jsonb[Segment] *: int8).imap((s, version) => s.copy(version = version))(s => (s, s.version))
