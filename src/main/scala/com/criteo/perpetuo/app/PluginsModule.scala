package com.criteo.perpetuo.app

import com.criteo.perpetuo.config._
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule

class PluginsModule extends TwitterModule {

  private val plugins = new Plugins()

  @Singleton
  @Provides
  def providesPlugins: Plugins = plugins

}
