package com.criteo.perpetuo.app

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.logging.modules.Slf4jBridgeModule


object CustomServerModules {
  val jackson = CustomJacksonModule
}

/**
  * Main server.
  */
class Server extends HttpServer {

  val version: String = AppConfig.get("version")

  override protected def jacksonModule = CustomServerModules.jackson

  override def defaultFinatraHttpPort: String = s":${AppConfig.get[Int]("http.port")}"

  override def modules = Seq(
    Slf4jBridgeModule,
    new DbContextModule(AppConfig.db)
  )

  override def configureHttp(router: HttpRouter) {
    if (AppConfig.get[Boolean]("logging")) {
      // Activate "Mapped Diagnostic Context" and access and stats logging
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
    }

    router
      .add[RestController]

      // Add controller for serving static assets as the last one / fallback one
      .add(new StaticAssetsController())
  }
}

object main extends Server
