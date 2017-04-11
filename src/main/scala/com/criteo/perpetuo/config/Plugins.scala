package com.criteo.perpetuo.config

import java.io.InputStream
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.dispatchers.TargetDispatcher

import scala.io.Source


object Plugins {
  private val factory = new ScriptEngineManager
  private val engine: ScriptEngine = factory.getEngineByName("groovy")
  assert(engine != null)

  lazy val dispatcher: TargetDispatcher = loadClassFromGroovy("plugins.dispatcher").newInstance()

  def loadClassFromGroovy[T](configPath: String): Class[T] = {
    val stream: InputStream = getClass.getResourceAsStream(AppConfig.get(configPath))
    engine.eval(Source.fromInputStream(stream).reader()).asInstanceOf[Class[T]]
  }
}
