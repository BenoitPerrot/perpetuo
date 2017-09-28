package com.criteo.perpetuo.config

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}

import scala.collection.JavaConverters._


abstract class BaseAppConfig {
  protected val config: Config

  private lazy val effectiveConfig: Config = if (config.hasPath(env)) config.getConfig(env).withFallback(config) else config

  val env: String

  def under(path: String): AppConfig = {
    new AppConfig(effectiveConfig.getConfig(path), env)
  }

  def get[T](path: String): T = {
    val v = effectiveConfig.getValue(path) match {
      case conf: ConfigObject => conf.get(env)
      case value => value
    }
    v.unwrapped() match {
      case arr: java.util.ArrayList[_] => arr.asScala
      case value => value
    }
  }.asInstanceOf[T]

  def tryGet[T](path: String): Option[T] = if (effectiveConfig.hasPath(path)) Some(get(path)) else None
}


class AppConfig(override protected val config: Config,
                override val env: String) extends BaseAppConfig


abstract class RootAppConfig extends BaseAppConfig {

  // todo: remove once new workflow is completely in place <<
  lazy val productsExcludedFromNewWorkflow: Seq[String] = tryGet("productsExcludedFromNewWorkflow").getOrElse(Seq())

  def isCoveredByOldWorkflow(productName: String): Boolean =
    productsExcludedFromNewWorkflow.contains(productName)
  // >>

  override lazy val env: String = config.getString("env")

  lazy val db: AppConfig = under("db")
}


object AppConfig extends RootAppConfig {
  override protected lazy val config: Config = ConfigFactory.load()
}
