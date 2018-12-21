package com.criteo.perpetuo.auth

import com.criteo.perpetuo.app.AuthModule
import com.criteo.perpetuo.config.AppConfig
import com.google.inject.{Provides, Singleton}
import com.twitter.finagle.http.Status.{Forbidden, Ok, Unauthorized}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{EmbeddedHttpServer, HttpServer}
import com.twitter.inject.{Test, TwitterModule}
import com.typesafe.config.Config


/**
  * Test the [[Controller]]
  *
  * Ensure the [[Controller]] is able to read the JWT from the dedicated cookie and to decode it on the appropriate
  * routes
  */
class ControllerSpec extends Test {

  val authModule = new AuthModule(AppConfig.config.getConfig("auth"))

  val server = new EmbeddedHttpServer(new HttpServer {

    override def modules = Seq(
      authModule,
      new TwitterModule {
        @Singleton
        @Provides
        def providesIdentityProvider: IdentityProvider = AnonymousIdentityProvider

        @Singleton
        @Provides
        def providesPermissions: Permissions = Unrestricted

        @Singleton
        @Provides
        def providesConfig: Config = AppConfig.config

        @Singleton
        @Provides
        def providesAppConfig: AppConfig = AppConfig
      }
    )

    override def configureHttp(router: HttpRouter): Unit = {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .filter[UserFilter]
        .add[Controller]
        .add[LocalUsersRetrievingController]
    }
  })

  private val knownUser = User("knownUser")
  private val knownUserJWT = knownUser.toJWT(authModule.jwtEncoder)

  test("The auth controller accepts valid token") {
    server.httpGet("/api/auth/identity",
      headers = Map("Cookie" -> s"jwt=$knownUserJWT"),
      andExpect = Ok
    )
  }

  test("The auth controller rejects invalid token") {
    server.httpGet("/api/auth/identity",
      headers = Map("Cookie" -> "jwt=DEADBEEF"),
      andExpect = Unauthorized
    )
  }

  test("The auth controller serves the JWT of known local-users") {
    server.httpGet("/api/auth/local-users/anonymous/jwt",
      headers = Map("Cookie" -> s"jwt=$knownUserJWT"),
      andExpect = Ok
    )
  }

  test("The auth controller doesn't serve the JWT of unknown local-users") {
    server.httpGet("/api/auth/local-users/unknown/jwt",
      headers = Map("Cookie" -> s"jwt=$knownUserJWT"),
      andExpect = Forbidden
    )
  }

}
