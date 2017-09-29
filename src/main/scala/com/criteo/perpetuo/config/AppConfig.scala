package com.criteo.perpetuo.config

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._


abstract class BaseAppConfig {
  protected val config: Config

  def under(path: String): AppConfig = {
    new AppConfig(config.getConfig(path))
  }

  def get[T](path: String): T = {
    config.getValue(path).unwrapped() match {
      case arr: java.util.ArrayList[_] => arr.asScala
      case value => value
    }
  }.asInstanceOf[T]

  def tryGet[T](path: String): Option[T] = if (config.hasPath(path)) Some(get(path)) else None
}


class AppConfig(override protected val config: Config) extends BaseAppConfig


abstract class RootAppConfig extends BaseAppConfig {

  lazy val db: AppConfig = under("db")
}


object AppConfig extends RootAppConfig {
  override protected lazy val config: Config = ConfigFactory.load()
}
