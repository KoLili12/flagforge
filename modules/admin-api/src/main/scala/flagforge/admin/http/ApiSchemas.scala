package flagforge.admin.http

import flagforge.admin.AdminError
import flagforge.admin.service.*
import flagforge.domain.*
import io.circe.Codec as JsonCodec
import sttp.tapir.{Codec, CodecFormat, DecodeResult, Schema}
import sttp.tapir.generic.Configuration
import sttp.tapir.json.circe.schemaForCirceJson

/** OpenAPI schemas and path codecs for domain types. Mirrors the circe conventions of the domain module. */
object ApiSchemas:

  given Configuration = Configuration.default.withDiscriminator("type").withSnakeCaseDiscriminatorValues

  // --- identifiers: plain strings in docs, validated when decoding path parameters ---

  private def keyCodec[T](
      either: String => Either[String, T],
      show: T => String
  ): Codec[String, T, CodecFormat.TextPlain] =
    Codec.string.mapDecode(s =>
      either(s).fold(err => DecodeResult.Error(s, IllegalArgumentException(err)), DecodeResult.Value(_))
    )(show)

  given Codec[String, EnvironmentKey, CodecFormat.TextPlain] = keyCodec(EnvironmentKey.either, _.value)
  given Codec[String, FlagKey, CodecFormat.TextPlain]        = keyCodec(FlagKey.either, _.value)
  given Codec[String, SegmentKey, CodecFormat.TextPlain]     = keyCodec(SegmentKey.either, _.value)

  given Schema[EnvironmentKey] = Schema.string
  given Schema[FlagKey]        = Schema.string
  given Schema[VariationKey]   = Schema.string
  given Schema[SegmentKey]     = Schema.string
  given Schema[RuleId]         = Schema.string

  // --- enums: documented with the exact strings circe produces ---

  private inline def stringEnum[T](using enc: io.circe.Encoder[T]): Schema[T] =
    Schema.derivedEnumeration[T](encode = Some(v => enc(v).asString.getOrElse(v.toString)))

  given Schema[FlagKind]  = stringEnum
  given Schema[Operator]  = stringEnum
  given Schema[ErrorKind] = stringEnum

  // --- domain ---

  given Schema[Variation]         = Schema.derived
  given Schema[FlagDefinition]    = Schema.derived
  given Schema[Prerequisite]      = Schema.derived
  given Schema[Target]            = Schema.derived
  given Schema[Clause]            = Schema.derived
  given Schema[WeightedVariation] = Schema.derived
  given Schema[Serve]             = Schema.derived
  given Schema[Rule]              = Schema.derived
  given Schema[FlagConfig]        = Schema.derived
  given Schema[FeatureFlag]       = Schema.derived
  given Schema[SegmentRule]       = Schema.derived
  given Schema[Segment]           = Schema.derived
  given Schema[Environment]       = Schema.derived
  given Schema[EvaluationContext] = Schema.derived
  given Schema[Reason]            = Schema.derived
  given Schema[Evaluation]        = Schema.derived

  given flagMapSchema: Schema[Map[FlagKey, FeatureFlag]] = Schema.schemaForMap[FlagKey, FeatureFlag](_.value)
  given segmentMapSchema: Schema[Map[SegmentKey, Segment]] = Schema.schemaForMap[SegmentKey, Segment](_.value)
  given Schema[Ruleset]                                    = Schema.derived

  // --- requests ---

  given Schema[CreateFlagRequest]       = Schema.derived
  given Schema[UpdateFlagConfigRequest] = Schema.derived
  given Schema[ToggleFlagRequest]       = Schema.derived
  given Schema[SaveSegmentRequest]      = Schema.derived

  // --- errors ---

  given JsonCodec.AsObject[AdminError.NotFound] = JsonCodec.AsObject.derived
  given JsonCodec.AsObject[AdminError.Conflict] = JsonCodec.AsObject.derived
  given JsonCodec.AsObject[AdminError.Invalid]  = JsonCodec.AsObject.derived
  given Schema[AdminError.NotFound]             = Schema.derived
  given Schema[AdminError.Conflict]             = Schema.derived
  given Schema[AdminError.Invalid]              = Schema.derived
