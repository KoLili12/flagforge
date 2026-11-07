package flagforge.admin.http

import flagforge.admin.{Actor, AdminError}
import flagforge.admin.http.ApiSchemas.given
import flagforge.admin.service.*
import flagforge.domain.*
import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.json.circe.*

/** Endpoint descriptions only, no logic. They drive both the HTTP server and the OpenAPI docs. */
object Endpoints:

  private val errors: EndpointOutput.OneOf[AdminError, AdminError] =
    oneOf[AdminError](
      oneOfVariant(StatusCode.NotFound, jsonBody[AdminError.NotFound].description("Entity does not exist")),
      oneOfVariant(StatusCode.Conflict, jsonBody[AdminError.Conflict].description("Duplicate key or stale version")),
      oneOfVariant(StatusCode.UnprocessableEntity, jsonBody[AdminError.Invalid].description("Validation failed"))
    )

  private val actor: EndpointInput[Actor] =
    header[Option[String]]("X-Actor")
      .description("Who makes the change; recorded in the audit log")
      .map(_.filter(_.nonEmpty).fold(Actor.Anonymous)(Actor(_)))(a => Some(a.name))

  private val api = endpoint.in("api" / "v1").errorOut(errors)

  private val envPath     = path[EnvironmentKey]("env").description("Environment key")
  private val flagPath    = path[FlagKey]("flag").description("Flag key")
  private val segmentPath = path[SegmentKey]("segment").description("Segment key")

  private val environmentsApi = api.in("environments").tag("Environments")
  private val flagsApi        = api.in("flags").tag("Flags")
  private val envFlagsApi     = api.in("environments" / envPath / "flags").tag("Flag targeting")
  private val segmentsApi     = api.in("environments" / envPath / "segments").tag("Segments")

  // Environments

  val listEnvironments = environmentsApi.get
    .out(jsonBody[List[Environment]])
    .summary("List environments")

  val createEnvironment = environmentsApi.post
    .in(actor)
    .in(jsonBody[Environment])
    .out(statusCode(StatusCode.Created).and(jsonBody[Environment]))
    .summary("Create an environment")
    .description("Every existing flag gets a disabled configuration in the new environment.")

  // Flags (environment independent)

  val listFlags = flagsApi.get
    .in(query[Boolean]("archived").default(false).description("Include archived flags"))
    .out(jsonBody[List[FlagDefinition]])
    .summary("List flags")

  val getFlag = flagsApi.get
    .in(flagPath)
    .out(jsonBody[FlagDefinition])
    .summary("Get a flag definition")

  val createFlag = flagsApi.post
    .in(actor)
    .in(jsonBody[CreateFlagRequest])
    .out(statusCode(StatusCode.Created).and(jsonBody[FlagDefinition]))
    .summary("Create a flag")
    .description("The flag starts disabled in every environment.")

  val archiveFlag = flagsApi.delete
    .in(flagPath)
    .in(actor)
    .out(statusCode(StatusCode.NoContent))
    .summary("Archive a flag")

  // Per-environment targeting

  val listFlagConfigs = envFlagsApi.get
    .out(jsonBody[List[FeatureFlag]])
    .summary("List flags with their targeting in an environment")

  val getFlagConfig = envFlagsApi.get
    .in(flagPath)
    .out(jsonBody[FeatureFlag])
    .summary("Get targeting of a flag")

  val updateFlagConfig = envFlagsApi.put
    .in(flagPath)
    .in(actor)
    .in(jsonBody[UpdateFlagConfigRequest])
    .out(jsonBody[FeatureFlag])
    .summary("Replace targeting of a flag")
    .description("Optimistic locking: `version` must equal the current version, otherwise 409 is returned.")

  val toggleFlag = envFlagsApi.post
    .in(flagPath / "toggle")
    .in(actor)
    .in(jsonBody[ToggleFlagRequest])
    .out(jsonBody[FeatureFlag])
    .summary("Turn a flag on or off (kill switch)")

  // Segments

  val listSegments = segmentsApi.get
    .out(jsonBody[List[Segment]])
    .summary("List segments")

  val getSegment = segmentsApi.get
    .in(segmentPath)
    .out(jsonBody[Segment])
    .summary("Get a segment")

  val saveSegment = segmentsApi.put
    .in(segmentPath)
    .in(actor)
    .in(jsonBody[SaveSegmentRequest])
    .out(jsonBody[Segment])
    .summary("Create (version 0) or update a segment")

  // Data plane preview

  val ruleset = api.get
    .in("environments" / envPath / "ruleset")
    .out(jsonBody[Ruleset])
    .tag("Evaluation")
    .summary("Full snapshot of an environment, as SDKs will receive it")

  val evaluate = api.post
    .in("environments" / envPath / "evaluate")
    .in(jsonBody[EvaluationContext])
    .out(jsonBody[List[Evaluation]])
    .tag("Evaluation")
    .summary("Evaluate all flags for a context")

  val health = endpoint.get.in("health").out(stringBody).tag("Ops").summary("Liveness probe")
