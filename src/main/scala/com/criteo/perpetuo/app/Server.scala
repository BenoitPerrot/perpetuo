package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.{LocalUsersRetrievingController, Controller => AuthenticationController}
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.metrics
import com.criteo.perpetuo.metrics.HttpMonitoringFilter
import com.jakehschwartz.finatra.swagger.DocsController
import com.samstarling.prometheusfinagle.PrometheusStatsReceiver
import com.samstarling.prometheusfinagle.metrics.Telemetry
import com.twitter.finagle.http.filter.Cors
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Http, SimpleFilter}
import com.twitter.finatra.http.filters.{LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{HttpServer, Controller => BaseController}
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.server.AdminHttpServer.Route
import io.prometheus.client.CollectorRegistry


object CustomServerModules {
  val jackson = CustomJacksonModule
}

/**
  * Main server.
  */
class Server extends HttpServer {
  private val registry: CollectorRegistry = CollectorRegistry.defaultRegistry

  // Remove unwanted admin routes
  override protected def routes: Seq[Route] = super.routes.filter(_.path != "/admin/registry.json")

  override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

  private val appConfig: AppConfig = AppConfig

  override def modules = Seq(
    new DbContextModule(appConfig.config.getConfig("db")),
    new PluginsModule(appConfig),
    new AuthModule(appConfig.config.getConfig("auth")),
    new SwaggerModule()
  )

  override def configureHttpServer(server: Http.Server): Http.Server = {
    val statsReceiver = new PrometheusStatsReceiver(registry)

    server
      .withStatsReceiver(statsReceiver)
      .withHttpStats
  }

  override def configureHttp(router: HttpRouter): Unit = {
    //TODO: Allow better CORS policy tuning
    //Allow CORS for custom UIs, note that beforeRouting is necessary to handle pre-flight requests
    router.filter(new Cors.HttpFilter(Cors.UnsafePermissivePolicy), beforeRouting = true)

    if (appConfig.config.getBoolean("logging")) {
      // Activate "Mapped Diagnostic Context" and access and stats logging
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
    }

    // Record metrics on API access
    router.filter(new HttpMonitoringFilter(new Telemetry(registry, "api")))

    appConfig.config
      .tryGetStringList("extraFilters").getOrElse(Seq())
      .:+(appConfig.config.getString("auth.filter"))
      .foreach(extraFilterClassName =>
        router.filter(injector.instance(Class.forName(extraFilterClassName)).asInstanceOf[SimpleFilter[Request, Response]])
      )

    appConfig.config
      .tryGetStringList("extraControllers").getOrElse(Seq())
      .:+(appConfig.config.getString("auth.controller"))
      .foreach(extraControllerClassName =>
        router.add(injector.instance(Class.forName(extraControllerClassName)).asInstanceOf[BaseController])
      )

    router
      .add[AuthenticationController]
      .add[LocalUsersRetrievingController]
      .add[RestController]
      .add[metrics.Controller]
      .add[DocsController]

      // Add controller for serving static assets as the last one / fallback one
      .add(new StaticAssetsController(appConfig.config.tryGetStringList("staticAssets.roots").getOrElse(Seq())))
  }
}

object main extends Server
