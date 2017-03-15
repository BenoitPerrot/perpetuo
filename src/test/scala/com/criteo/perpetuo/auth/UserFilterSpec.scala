package com.criteo.perpetuo.auth

import com.criteo.perpetuo.app.{AppConfig, AuthModule}
import com.criteo.perpetuo.auth.UserFilter._
import com.twitter.finagle.http.Status.{Forbidden, Ok}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.finatra.http.{HttpServer, Controller => BaseController}
import com.twitter.inject.server.FeatureTest

/**
  * Test the [[UserFilter]].
  *
  * Ensure the [[UserFilter]] adds a User instance to requests when the JWT authentication cookie is found
  */
class UserFilterSpec extends FeatureTest {

  val authModule = new AuthModule(AppConfig.under("auth"))

  val server = new EmbeddedHttpServer(new HttpServer {

    override def modules = Seq(authModule)

    override def configureHttp(router: HttpRouter) {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        // Add the UserFilter and a controller checking the decoration of requests
        .filter[UserFilter]
        .add(new BaseController() {
          get("/username-from-jwt-cookie") { r: Request =>
            r.user.map(u => response.ok.plain(u.name)).getOrElse(response.forbidden)
          }
        })
    }
  })

  val knownUser = User("knownUser")
  val knownUserJWT = knownUser.toJWT(authModule.jwtEncoder)

  "The UserFilter" should {

    "decorate requests with a known user when the JWT cookie is valid" in {
      server.httpGet("/username-from-jwt-cookie",
        headers = Map("Cookie" -> s"jwt=$knownUserJWT"),
        andExpect = Ok,
        withBody = knownUser.name
      )
    }

    "decorate requests with the anonymous user when the JWT cookie is not set" in {
      server.httpGet("/username-from-jwt-cookie",
        andExpect = Ok,
        withBody = User.anonymous.name
      )
    }

    "fail when the JWT cookie is invalid" in {
      server.httpGet("/username-from-jwt-cookie",
        headers = Map("Cookie" -> "jwt=DEADBEEF"),
        andExpect = Forbidden
      )
    }
  }
}
