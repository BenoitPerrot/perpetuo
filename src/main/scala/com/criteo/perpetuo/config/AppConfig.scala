package com.criteo.perpetuo.config

import com.typesafe.config.Config

class AppConfig(val config: Config) {
  val selfUrl: String = config.getString("selfUrl")

  val executorsConfig: Config = config.getConfig("executors")
  def executorConfig(executorName: String): Config = executorsConfig.getConfig(executorName)
}
