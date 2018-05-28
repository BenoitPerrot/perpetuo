package com.criteo.perpetuo.config

import com.typesafe.config.{Config, ConfigException}

import scala.collection.JavaConverters._
import scala.reflect._

object ConfigSyntacticSugar {

  implicit class ConfigSyntacticSugar(val config: Config) {
    def tryGet[T: ClassTag](path: String): Option[T] = if (config.hasPath(path)) Some(get[T](path)) else None

    def getOrElse[T: ClassTag](path: String, default: => T): T = if (config.hasPath(path)) get[T](path) else default

    def get[T: ClassTag](path: String): T = {
      val v = config.getValue(path).unwrapped() match {
        case arr: java.util.ArrayList[_] => arr.asScala
        case obj: java.util.Map[_, _] => obj.asScala
        case value => value
      }
      try {
        v.asInstanceOf[T]
      } catch {
        case _: ClassCastException =>
          throw new ConfigException.WrongType(config.origin, s"Expected a ${classTag[T].runtimeClass.getName}, got $v (${v.getClass.getName}")
      }
    }

    def tryGetConfig(path: String): Option[Config] = if (config.hasPath(path)) Some(config.getConfig(path)) else None

    def tryGetConfigList(path: String): Option[Seq[Config]] = if (config.hasPath(path)) Some(config.getConfigList(path).asScala) else None
  }

}
