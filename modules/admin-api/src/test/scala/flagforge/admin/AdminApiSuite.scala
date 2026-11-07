package flagforge.admin

import cats.effect.IO
import io.circe.Json
import io.circe.literal.*
import io.circe.parser.parse
import org.http4s.*
import org.http4s.headers.`Content-Type`
import org.typelevel.ci.*

/** End-to-end: HTTP request → tapir → service → Postgres, without opening a port. */
class AdminApiSuite extends PostgresSuite:

  private final case class Response(status: Status, body: Json)

  private def withApp[A](f: HttpApp[IO] => IO[A]): IO[A] =
    withContainers(pg => AdminApp.httpApp[IO](dbConfig(pg)).use(f))

  private def call(app: HttpApp[IO], method: Method, path: String, body: Option[Json] = None): IO[Response] =
    val base = Request[IO](method, Uri.unsafeFromString(path)).putHeaders(Header.Raw(ci"X-Actor", "test-suite"))
    val request = body.fold(base)(json =>
      base.withEntity(json.noSpaces).withContentType(`Content-Type`(MediaType.application.json))
    )
    app.run(request).flatMap: response =>
      response.as[String].map(text => Response(response.status, parse(text).getOrElse(Json.fromString(text))))

  extension (json: Json)
    private def at(path: String*): Json =
      path.foldLeft(json)((j, field) => j.hcursor.downField(field).focus.getOrElse(Json.Null))

  private val checkoutFlag =
    json"""{
      "key": "new-checkout",
      "name": "New checkout",
      "kind": "boolean",
      "variations": [ { "key": "on", "value": true }, { "key": "off", "value": false } ]
    }"""

  private def checkoutConfig(version: Long, enabled: Boolean = true, ruleVariation: String = "on") =
    json"""{
      "version": $version,
      "config": {
        "enabled": $enabled,
        "offVariation": "off",
        "fallthrough": { "type": "fixed", "variation": "off" },
        "rules": [{
          "id": "ru-users",
          "clauses": [ { "attribute": "country", "operator": "in", "values": ["RU"] } ],
          "serve": { "type": "fixed", "variation": $ruleVariation }
        }]
      }
    }"""

  private def evaluate(app: HttpApp[IO], country: String): IO[Json] =
    call(app, Method.POST, "/api/v1/environments/production/evaluate", Some(json"""{"key": "user-1", "attributes": {"country": $country}}"""))
      .map(_.body.asArray.flatMap(_.find(_.at("flag") == Json.fromString("new-checkout"))).getOrElse(Json.Null))

  test("full flag lifecycle over HTTP"):
    withApp: app =>
      for
        env <- call(app, Method.POST, "/api/v1/environments", Some(json"""{"key": "production", "name": "Production"}"""))
        _ = assertEquals(env.status, Status.Created)

        created <- call(app, Method.POST, "/api/v1/flags", Some(checkoutFlag))
        _ = assertEquals(created.status, Status.Created, created.body)
        _ = assert(created.body.at("salt").isString)

        initial <- evaluate(app, "RU")
        _ = assertEquals(initial.at("value"), Json.False, "a new flag starts disabled")
        _ = assertEquals(initial.at("reason", "type"), Json.fromString("off"))

        updated <- call(app, Method.PUT, "/api/v1/environments/production/flags/new-checkout", Some(checkoutConfig(1)))
        _ = assertEquals(updated.status, Status.Ok, updated.body)
        _ = assertEquals(updated.body.at("version"), Json.fromLong(2))

        ru <- evaluate(app, "RU")
        de <- evaluate(app, "DE")
        _ = assertEquals(ru.at("value"), Json.True)
        _ = assertEquals(ru.at("reason", "type"), Json.fromString("rule_match"))
        _ = assertEquals(de.at("value"), Json.False)
        _ = assertEquals(de.at("reason", "type"), Json.fromString("fallthrough"))

        stale <- call(app, Method.PUT, "/api/v1/environments/production/flags/new-checkout", Some(checkoutConfig(1)))
        _ = assertEquals(stale.status, Status.Conflict)

        invalid <- call(
          app,
          Method.PUT,
          "/api/v1/environments/production/flags/new-checkout",
          Some(checkoutConfig(2, ruleVariation = "ghost"))
        )
        _ = assertEquals(invalid.status, Status.UnprocessableContent)
        _ = assert(invalid.body.at("errors").noSpaces.contains("unknown variation 'ghost'"), invalid.body)

        killed <- call(app, Method.POST, "/api/v1/environments/production/flags/new-checkout/toggle", Some(json"""{"enabled": false}"""))
        _ = assertEquals(killed.body.at("config", "enabled"), Json.False)
        afterKill <- evaluate(app, "RU")
        _ = assertEquals(afterKill.at("value"), Json.False)

        ruleset <- call(app, Method.GET, "/api/v1/environments/production/ruleset")
        _ = assertEquals(ruleset.body.at("flags", "new-checkout", "version"), Json.fromLong(3))
      yield ()

  test("errors map to proper status codes"):
    withApp: app =>
      for
        missingFlag <- call(app, Method.GET, "/api/v1/flags/does-not-exist")
        missingEnv  <- call(app, Method.GET, "/api/v1/environments/nowhere/flags")
        badKey      <- call(app, Method.GET, "/api/v1/flags/Not%20A%20Key")
        badFlag <- call(
          app,
          Method.POST,
          "/api/v1/flags",
          Some(json"""{"key": "broken", "name": "Broken", "kind": "boolean", "variations": [{"key": "yes", "value": "yes"}]}""")
        )
      yield
        assertEquals(missingFlag.status, Status.NotFound)
        assertEquals(missingEnv.status, Status.NotFound)
        assertEquals(badKey.status, Status.BadRequest)
        assertEquals(badFlag.status, Status.UnprocessableContent)

  test("OpenAPI docs are served"):
    withApp: app =>
      call(app, Method.GET, "/docs/docs.yaml").map: response =>
        assertEquals(response.status, Status.Ok)
        assert(response.body.asString.exists(_.contains("flagforge admin API")))
