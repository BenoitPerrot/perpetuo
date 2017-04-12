package com.criteo.perpetuo.config

import java.io.InputStreamReader
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.dispatchers.TargetDispatcher


object Plugins {
  lazy val dispatcher: TargetDispatcher = loadClassFromGroovy(AppConfig.get("plugins.dispatcher")).newInstance()
  lazy val hooks: Hooks = AppConfig.tryGet("plugins.hooks")
    .map(loadClassFromGroovy[Hooks](_: String).newInstance())
    .getOrElse(new Hooks)


  private val factory = new ScriptEngineManager
  private val engine: ScriptEngine = factory.getEngineByName("groovy")
  assert(engine != null)

  def loadClassFromGroovy[T](scriptPath: String): Class[T] = {
    val resource = getClass.getResource(scriptPath)
    engine.eval(new InputStreamReader(resource.openStream())).asInstanceOf[Class[T]]
  }
}
