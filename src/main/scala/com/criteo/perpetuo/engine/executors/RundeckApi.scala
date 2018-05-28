package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._


trait RundeckApi {
  val apiVersion = 16

  val host: String

  private val config = AppConfigProvider.executorConfig("rundeck")
  val port: Int = config.getIntOrElse("port", 80)
  val authToken: Option[String] = config.tryGetString("token")

  protected def apiPath(apiSubPath: String): String = {
    val path = s"/api/$apiVersion/$apiSubPath"
    authToken.map(t => s"$path?authtoken=$t").getOrElse(path)
  }
}
