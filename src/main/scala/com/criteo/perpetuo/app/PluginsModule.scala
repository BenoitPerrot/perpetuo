package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.{IdentityProvider, Permissions}
import com.criteo.perpetuo.config._
import com.criteo.perpetuo.engine.Listener
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.typesafe.config.Config


class PluginsModule(config: Config) extends TwitterModule {
  private val plugins = new Plugins(config)

  @Singleton
  @Provides
  def providesTargetResolver: TargetResolver = plugins.resolver

  @Singleton
  @Provides
  def providesTargetDispatcher: TargetDispatcher = plugins.dispatcher

  @Singleton
  @Provides
  def providesIdentityProvider: IdentityProvider = plugins.identityProvider

  @Singleton
  @Provides
  def providesPermissions: Permissions = plugins.permissions

  @Singleton
  @Provides
  def providesListeners: Seq[Listener] = plugins.listeners
}
