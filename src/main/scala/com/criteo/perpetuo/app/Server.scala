package com.criteo.perpetuo.app

import ch.qos.logback.classic.util.ContextInitializer
import com.criteo.perpetuo.auth.{UserFilter, Controller => AuthenticationController}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{HttpServer, Controller => BaseController}
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.server.AdminHttpServer.Route
import com.typesafe.config.Config


object CustomServerModules {
  val jackson = CustomJacksonModule
}

trait ServerConfigurator {
  val config: Config = AppConfigProvider.config

  config.tryGetString("logback.configurationFile").foreach(
    System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, _)
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

  override def configureHttp(router: HttpRouter) {
    if (config.getBoolean("logging")) {
      // Activate "Mapped Diagnostic Context" and access and stats logging
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
    }
    router.filter[UserFilter]

    config.tryGetStringList("extraControllers")
      .foreach(_
        .foreach(extraControllerClassName =>
          router.add(injector.instance(Class.forName(extraControllerClassName)).asInstanceOf[BaseController])
        )
      )

    router
      .add[AuthenticationController]
      .add[RestController]

      // Add controller for serving static assets as the last one / fallback one
      .add(new StaticAssetsController(config.tryGetStringList("staticAssets.roots").getOrElse(Seq())))
  }
}

object main extends Server
