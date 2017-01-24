package com.criteo.perpetuo.app

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValueFactory}


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
}


class AppConfig(override protected val config: Config,
                override val env: String) extends BaseAppConfig


abstract class RootAppConfig extends BaseAppConfig {
  protected val unresolvedConfig: Config

  override protected lazy val config: Config = unresolvedConfig.resolve

  override lazy val env: String = config.getString(envPath)
  private val envPath = "env"

  lazy val db: AppConfig = under("db")

  def withEnv(overriddenEnv: String) = new LoadedRootAppConfig(
    // replace the environment in the unresolved configuration in order to impact all dependant values
    unresolvedConfig.withValue(envPath, ConfigValueFactory.fromAnyRef(overriddenEnv))
  )
}


class LoadedRootAppConfig(override protected val unresolvedConfig: Config) extends RootAppConfig


object AppConfig extends RootAppConfig {
  override protected lazy val unresolvedConfig: Config = ConfigFactory.parseFile(
    new File(Thread.currentThread.getContextClassLoader.getResource("application.conf").getFile)
  )
}
