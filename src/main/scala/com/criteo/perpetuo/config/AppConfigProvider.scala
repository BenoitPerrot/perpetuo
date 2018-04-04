package com.criteo.perpetuo.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfigProvider {

  val config: Config = ConfigFactory.load().resolve()

  val executorsConfig: Config = config.getConfig("executors")
  def executorConfig(executorName: String): Config = executorsConfig.getConfig(executorName)

}
