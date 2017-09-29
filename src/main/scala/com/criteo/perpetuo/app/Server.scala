package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.{UserFilter, Controller => AuthenticationController}
import com.criteo.perpetuo.config.AppConfig
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{HttpServer, Controller => BaseController}
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.finatra.logging.modules.Slf4jBridgeModule


object CustomServerModules {
  val jackson = CustomJacksonModule
}

/**
  * Main server.
  */
class Server extends HttpServer {

  override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

  override def defaultFinatraHttpPort: String = s":${AppConfig.get[Int]("http.port")}"

  override def modules = Seq(
    Slf4jBridgeModule,
    new DbContextModule(AppConfig.db),
    new PluginsModule(AppConfig),
    new AuthModule(AppConfig.getConfig("auth"))
  )

  override def configureHttp(router: HttpRouter) {
    if (AppConfig.get("logging")) {
      // Activate "Mapped Diagnostic Context" and access and stats logging
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
    }
    router.filter[UserFilter]

    AppConfig.tryGet[Iterable[String]]("extraControllers")
      .foreach(_
        .foreach(extraControllerClassName =>
          router.add(injector.instance(Class.forName(extraControllerClassName)).asInstanceOf[BaseController])
        )
      )

    router
      .add[AuthenticationController]
      .add[RestController]

      // Add controller for serving static assets as the last one / fallback one
      .add(new StaticAssetsController(AppConfig.tryGet("staticAssets.roots").getOrElse(Seq())))
  }
}

object main extends Server
