package com.criteo.perpetuo.app

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.logging.modules.Slf4jBridgeModule
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Main server.
  */
class Server extends HttpServer {

  // Load default application.conf file
  val config: Config = ConfigFactory.load()

  val version = config.getString("version")

  override def defaultFinatraHttpPort: String = s":${config.getInt("http.port")}"

  override def modules = Seq(
    Slf4jBridgeModule
  )

  override def configureHttp(router: HttpRouter) {
    if (config.getBoolean("logging")) {
      // Activate "Mapped Diagnostic Context" and access and stats logging
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
    }

    router
      // Add controller for serving static assets as the last one / fallback one
      .add(new StaticAssetsController())
  }
}

object main extends Server
