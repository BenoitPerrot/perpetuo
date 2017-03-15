package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.UserFilter
import com.criteo.perpetuo.auth.{Controller => AuthenticationController}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.finatra.logging.modules.Slf4jBridgeModule


object CustomServerModules {
  val jackson = CustomJacksonModule
}

/**
  * Main server.
  *
  * While developing, you probably want finatra to load assets directly from the file system to allow a faster
  * development cycle (indeed, by default, assets will be served as resources, meaning that the server must be
  * stopped then recompiled before any modified asset is actually served). To do that, just add the following
  * option to the command line: -local.doc.root=src/main/webapp
  *
  * Notably in IntelliJ, "Edit the run/debug Configurations", set the "Working Directory" to the actual root of the project
  * (and *not* to the root of the workspace), and add the option above in the "Program Arguments" field.
  */
class Server extends HttpServer {

  override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

  override def defaultFinatraHttpPort: String = s":${AppConfig.get[Int]("http.port")}"

  override def modules = Seq(
    Slf4jBridgeModule,
    new DbContextModule(AppConfig.db),
    new AuthModule(AppConfig.under("auth"))
  )

  override def configureHttp(router: HttpRouter) {
    if (AppConfig.get("logging")) {
      // Activate "Mapped Diagnostic Context" and access and stats logging
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
    }

    router
      .filter[UserFilter]
      .add[AuthenticationController]
      .add[RestController]

      // Add controller for serving static assets as the last one / fallback one
      .add(new StaticAssetsController())
  }
}

object main extends Server
