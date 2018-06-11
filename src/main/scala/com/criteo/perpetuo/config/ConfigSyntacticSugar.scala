package com.criteo.perpetuo.config

import com.typesafe.config.Config

import scala.collection.JavaConverters._

object ConfigSyntacticSugar {

  implicit class ConfigSyntacticSugar(val config: Config) {
    def tryGetConfig(path: String): Option[Config] =
      if (config.hasPath(path)) Some(config.getConfig(path)) else None

    def tryGetConfigList(path: String): Option[Seq[Config]] =
      if (config.hasPath(path)) Some(config.getConfigList(path).asScala) else None

    def tryGetStringList(path: String): Option[Seq[String]] =
      if (config.hasPath(path)) Some(config.getStringList(path).asScala) else None

    def tryGetString(path: String): Option[String] =
      if (config.hasPath(path)) Some(config.getString(path)) else None

    def tryGetBoolean(path: String): Option[Boolean] =
      if (config.hasPath(path)) Some(config.getBoolean(path)) else None

    def getIntOrElse(path: String, default: Int): Int =
      if (config.hasPath(path)) config.getInt(path) else default
  }

}
