package com.criteo.perpetuo.auth

import com.criteo.perpetuo.app.AuthModule
import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.config.TestConfig
import com.google.inject.{Provides, Singleton}
import com.twitter.finagle.http.Status.{Ok, Unauthorized}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{EmbeddedHttpServer, HttpServer, Controller => BaseController}
import com.twitter.inject.{Test, TwitterModule}
import com.typesafe.config.Config


/**
  * Test the [[UserFilter]].
  *
  * Ensure the [[UserFilter]] adds a User instance to requests when the JWT authentication cookie is found
  */
class UserFilterSpec extends Test {

  val config: Config = TestConfig.config
  val authModule = new AuthModule(config.getConfig("auth"))

  val server = new EmbeddedHttpServer(new HttpServer {

    override def modules = Seq(
      authModule,
      new TwitterModule() {
        @Singleton
        @Provides
        def providesIdentityProvider: IdentityProvider = AnonymousIdentityProvider
      }
    )

    override def configureHttp(router: HttpRouter): Unit = {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        // Add the UserFilter and a controller checking the decoration of requests
        .filter[UserFilter]
        .add(new BaseController() {
          get("/username") { r: Request =>
            r.user.map(u => response.ok.plain(u.name)).getOrElse(response.unauthorized)
          }
        })
    }
  })

  private val knownUser = User("knownUser")
  private val knownUserJWT = knownUser.toJWT(authModule.jwtEncoder)

  test("The UserFilter decorates requests with a known user when the JWT cookie is valid") {
    server.httpGet("/username",
      headers = Map("Cookie" -> s"jwt=$knownUserJWT"),
      andExpect = Ok,
      withBody = knownUser.name
    )
  }

  test("The UserFilter decorates requests with a known user when the Authorization header contains a valid JWT")  {
    server.httpGet("/username",
      headers = Map("Authorization" -> knownUserJWT),
      andExpect = Ok,
      withBody = knownUser.name
    )
  }

  test("The UserFilter fails when the JWT cookie is not set") {
    server.httpGet("/username",
      andExpect = Unauthorized
    )
  }

  test("The UserFilter fails when the JWT cookie is invalid") {
    server.httpGet("/username",
      headers = Map("Cookie" -> "jwt=DEADBEEF"),
      andExpect = Unauthorized
    )
  }

  test("The UserFilter fails when the Authorization header contains an invalid JWT") {
    server.httpGet("/username",
      headers = Map("Authorization" -> "DEADBEEF"),
      andExpect = Unauthorized
    )
  }
}
