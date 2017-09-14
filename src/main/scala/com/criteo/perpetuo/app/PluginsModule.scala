package com.criteo.perpetuo.app

import com.criteo.perpetuo.config._
import com.criteo.perpetuo.engine.Listener
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule

class PluginsModule extends TwitterModule {

  private val plugins = new Plugins()

  @Singleton
  @Provides
  def providesEngineListener: Listener = plugins.listener

  @Singleton
  @Provides
  def providesTargetDispatcher: TargetDispatcher = plugins.dispatcher

}
