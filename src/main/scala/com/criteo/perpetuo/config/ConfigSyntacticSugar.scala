package com.criteo.perpetuo.config

import com.typesafe.config.Config
import scala.collection.JavaConverters._

object ConfigSyntacticSugar {

  implicit class ConfigSyntacticSugar(val config: Config) {
    def tryGet[T](path: String): Option[T] = if (config.hasPath(path)) Some(get[T](path)) else None

    def get[T](path: String): T = {
      config.getValue(path).unwrapped() match {
        case arr: java.util.ArrayList[_] => arr.asScala
        case value => value
      }
    }.asInstanceOf[T]
  }

}
