package com.criteo.perpetuo.config

import com.typesafe.config.Config

import scala.collection.JavaConverters._

object ConfigSyntacticSugar {

  implicit class ConfigSyntacticSugar(val config: Config) {
    def tryGet[T](path: String): Option[T] = if (config.hasPath(path)) Some(get[T](path)) else None

    def getOrElse[T](path: String, default: => T): T = if (config.hasPath(path)) get[T](path) else default

    def get[T](path: String): T = {
      config.getValue(path).unwrapped() match {
        case arr: java.util.ArrayList[_] => arr.asScala
        case obj: java.util.Map[_, _] => obj.asScala
        case value => value
      }
    }.asInstanceOf[T]

    def tryGetConfig(path: String): Option[Config] = if (config.hasPath(path)) Some(config.getConfig(path)) else None

    def tryGetConfigList(path: String): Option[Seq[Config]] = if (config.hasPath(path)) Some(config.getConfigList(path).asScala) else None
  }

}
