package com.criteo.perpetuo.auth

import com.criteo.perpetuo.app.{AppConfig, AuthModule}
import com.twitter.finagle.http.Status.{Forbidden, Ok}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.finatra.http.HttpServer
import com.twitter.inject.server.FeatureTest

/**
  * Test the [[Controller]]
  *
  * Ensure the [[Controller]] is able to read the JWT from the dedicated cookie and to decode it on the appropriate
  * routes
  */
class ControllerSpec extends FeatureTest {

  val authModule = new AuthModule(AppConfig.under("auth"))

  val server = new EmbeddedHttpServer(new HttpServer {

    override def modules = Seq(authModule)

    override def configureHttp(router: HttpRouter) {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .filter[UserFilter]
        .add[Controller]
    }
  })

  val knownUser = User("knownUser")
  val knownUserJWT = knownUser.toJWT(authModule.jwtEncoder)

  "A Server" should {

    "accept valid token" in {
      server.httpGet("/api/auth/identity",
        headers = Map("Cookie" -> s"jwt=$knownUserJWT"),
        andExpect = Ok
      )
    }

    "reject invalid token" in {
      server.httpGet("/api/auth/identity",
        headers = Map("Cookie" -> "jwt=DEADBEEF"),
        andExpect = Forbidden
      )
    }
  }

}
