package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.{IdentifyingController, LocalUsersRetrievingController, Controller => AuthenticationController}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.metrics
import com.samstarling.prometheusfinagle.PrometheusStatsReceiver
import com.twitter.finagle.http.filter.Cors
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Http, SimpleFilter}
import com.twitter.finatra.http.filters.{LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{HttpServer, Controller => BaseController}
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.server.AdminHttpServer.Route
import com.typesafe.config.Config
import io.prometheus.client.CollectorRegistry


object CustomServerModules {
  val jackson = CustomJacksonModule
}

trait ServerConfigurator {
  val config: Config = AppConfigProvider.config

  config.tryGetString("log4j.configurationFile").foreach(
    System.setProperty("log4j.configurationFile", _)
  )
}

/**
  * Main server.
  */
class Server extends ServerConfigurator with HttpServer {

  // Remove unwanted admin routes
  override protected def routes: Seq[Route] = super.routes.filter(_ != "/admin/registry.json")

  override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

  override def modules = Seq(
    new DbContextModule(config.getConfig("db")),
    new PluginsModule,
    new AuthModule(config.getConfig("auth"))
  )

  override def configureHttpServer(server: Http.Server): Http.Server = {
    val registry = CollectorRegistry.defaultRegistry
    val statsReceiver = new PrometheusStatsReceiver(registry)

    server
      .withStatsReceiver(statsReceiver)
      .withHttpStats
  }

  override def configureHttp(router: HttpRouter) {
    //TODO: Allow better CORS policy tuning
    //Allow CORS for custom UIs, note that beforeRouting is necessary to handle pre-flight requests
    router.filter(new Cors.HttpFilter(Cors.UnsafePermissivePolicy), beforeRouting = true)

    if (config.getBoolean("logging")) {
      // Activate "Mapped Diagnostic Context" and access and stats logging
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
    }

    config
      .tryGetStringList("extraFilters").getOrElse(Seq())
      .:+(config.getString("auth.filter"))
      .foreach(extraFilterClassName =>
        router.filter(injector.instance(Class.forName(extraFilterClassName)).asInstanceOf[SimpleFilter[Request, Response]])
      )

    config
      .tryGetStringList("extraControllers").getOrElse(Seq())
      .:+(config.getString("auth.controller"))
      .foreach(extraControllerClassName =>
        router.add(injector.instance(Class.forName(extraControllerClassName)).asInstanceOf[BaseController])
      )

    router
      .add[AuthenticationController]
      .add[LocalUsersRetrievingController]
      .add[RestController]
      .add[metrics.Controller]

      // Add controller for serving static assets as the last one / fallback one
      .add(new StaticAssetsController(config.tryGetStringList("staticAssets.roots").getOrElse(Seq())))
  }
}

object main extends Server
