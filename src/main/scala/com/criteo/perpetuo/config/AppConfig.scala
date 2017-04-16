package com.criteo.perpetuo.config

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}


abstract class BaseAppConfig {
  protected val config: Config

  private lazy val effectiveConfig: Config = if (config.hasPath(env)) config.getConfig(env).withFallback(config) else config

  val env: String

  def under(path: String): AppConfig = {
    new AppConfig(effectiveConfig.getConfig(path), env)
  }

  def get[T](path: String): T = {
    effectiveConfig.getValue(path) match {
      case conf: ConfigObject => conf.get(env)
      case value => value
    }
  }.unwrapped().asInstanceOf[T]

  def tryGet[T](path: String): Option[T] = if (effectiveConfig.hasPath(path)) Some(get(path)) else None
}


class AppConfig(override protected val config: Config,
                override val env: String) extends BaseAppConfig


abstract class RootAppConfig extends BaseAppConfig {
  override lazy val env: String = config.getString("env")

  lazy val db: AppConfig = under("db")
}


object AppConfig extends RootAppConfig {
  override protected lazy val config: Config = ConfigFactory.load("com/criteo/perpetuo/application.conf")

  def withValue(path: String, value: ConfigValue): RootAppConfig = {
    val newConfig = config.withValue(path, value)
    new RootAppConfig {
      override protected val config: Config = newConfig
    }
  }
}
