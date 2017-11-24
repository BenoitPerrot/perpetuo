package com.criteo.perpetuo.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfigProvider {

  val config: Config = ConfigFactory.load().resolve()

}
