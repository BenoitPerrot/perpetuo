package com.criteo.perpetuo.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {

  val config: Config = ConfigFactory.load().resolve()

  val selfUrl: String = config.getString("selfUrl")

  val executorsConfig: Config = config.getConfig("executors")
  def executorConfig(executorName: String): Config = executorsConfig.getConfig(executorName)

}
