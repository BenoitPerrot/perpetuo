package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.{IdentityProvider, Permissions}
import com.criteo.perpetuo.config.Plugins
import com.criteo.perpetuo.engine.Listener
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule


class PluginsModule extends TwitterModule {

  @Singleton
  @Provides
  def providesTargetResolver(plugins: Plugins): TargetResolver = plugins.resolver

  @Singleton
  @Provides
  def providesTargetDispatcher(plugins: Plugins): TargetDispatcher = plugins.dispatcher

  @Singleton
  @Provides
  def providesIdentityProvider(plugins: Plugins): IdentityProvider = plugins.identityProvider

  @Singleton
  @Provides
  def providesPermissions(plugins: Plugins): Permissions = plugins.permissions

  @Singleton
  @Provides
  def providesListeners(plugins: Plugins): Seq[Listener] = plugins.listeners
}
