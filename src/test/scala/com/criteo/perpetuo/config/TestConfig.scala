package com.criteo.perpetuo.config

import com.typesafe.config.ConfigFactory


object TestConfig extends AppConfig(ConfigFactory.load().resolve())
