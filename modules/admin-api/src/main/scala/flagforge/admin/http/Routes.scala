package flagforge.admin.http

import cats.effect.Async
import cats.syntax.all.*
import flagforge.admin.AdminError
import flagforge.admin.service.AdminService
import org.http4s.HttpRoutes
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter

/** Binds endpoint descriptions to the service. Swagger UI is served at `/docs`. */
final class Routes[F[_]: Async](service: AdminService[F]):
  import Endpoints.*

  extension [A](fa: F[A]) private def handled: F[Either[AdminError, A]] = fa.attemptNarrow[AdminError]

  val serverEndpoints: List[ServerEndpoint[Any, F]] = List(
    listEnvironments.serverLogic(_ => service.listEnvironments.handled),
    createEnvironment.serverLogic((actor, env) => service.createEnvironment(env, actor).handled),
    listFlags.serverLogic(archived => service.listFlags(archived).handled),
    getFlag.serverLogic(key => service.getFlag(key).handled),
    createFlag.serverLogic((actor, request) => service.createFlag(request, actor).handled),
    archiveFlag.serverLogic((key, actor) => service.archiveFlag(key, actor).handled),
    listFlagConfigs.serverLogic(env => service.listFlagConfigs(env).handled),
    getFlagConfig.serverLogic((env, key) => service.getFlagConfig(env, key).handled),
    updateFlagConfig.serverLogic((env, key, actor, request) => service.updateFlagConfig(env, key, request, actor).handled),
    toggleFlag.serverLogic((env, key, actor, request) => service.toggleFlag(env, key, request.enabled, actor).handled),
    listSegments.serverLogic(env => service.listSegments(env).handled),
    getSegment.serverLogic((env, key) => service.getSegment(env, key).handled),
    saveSegment.serverLogic((env, key, actor, request) => service.saveSegment(env, key, request, actor).handled),
    ruleset.serverLogic(env => service.ruleset(env).handled),
    evaluate.serverLogic((env, context) => service.evaluate(env, context).handled),
    health.serverLogicSuccess(_ => "ok".pure[F])
  )

  private val docs: List[ServerEndpoint[Any, F]] =
    SwaggerInterpreter().fromServerEndpoints[F](serverEndpoints, "flagforge admin API", "0.1.0")

  val routes: HttpRoutes[F] = Http4sServerInterpreter[F]().toRoutes(serverEndpoints ++ docs)
