package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.{IdentityProvider, Permissions}
import com.criteo.perpetuo.config.{AppConfig, PluginLoader}
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.engine.{AsyncListener, AsyncPreConditionEvaluator}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule

import scala.collection.JavaConversions._

class PluginsModule(appConfig: AppConfig) extends TwitterModule {

  private val config = appConfig.config

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  @Singleton
  @Provides
  def providesTargetResolver(loader: PluginLoader): TargetResolver =
    loader.loadTargetResolver(config.tryGetConfig("targetResolver"))

  @Singleton
  @Provides
  def providesTargetDispatcher(loader: PluginLoader): TargetDispatcher =
    loader.loadTargetDispatcher(config.tryGetConfig("targetDispatcher"))

  @Singleton
  @Provides
  def providesIdentityProvider(loader: PluginLoader): IdentityProvider =
    loader.loadIdentityProvider(config.tryGetConfig("auth.identityProvider"))

  @Singleton
  @Provides
  def providesPermissions(loader: PluginLoader): Permissions =
    loader.loadPermissions(config.tryGetConfig("permissions"))

  @Singleton
  @Provides
  def providesListeners(loader: PluginLoader): Seq[AsyncListener] =
    loader.loadListeners(if (config.hasPath("engineListeners")) config.getConfigList("engineListeners") else Seq())

  @Singleton
  @Provides
  def providesPreConditionEvaluators(loader: PluginLoader): Seq[AsyncPreConditionEvaluator] =
    loader.loadPreConditionEvaluators(if (config.hasPath("preConditionEvaluators")) config.getConfigList("preConditionEvaluators") else Seq())

  @Singleton
  @Provides
  def providesAppConfig: AppConfig = appConfig
}
