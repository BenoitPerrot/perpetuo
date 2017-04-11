package com.criteo.perpetuo.dispatchers

import java.io.InputStream
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.executors.ExecutorInvoker

import scala.io.Source


trait TargetDispatcher {
  def assign(selectWord: String): Set[ExecutorInvoker]
}


object TargetDispatcher {
  private val configPath = "groovyScripts.dispatcher"

  lazy val fromGroovy: TargetDispatcher = loadClassFromGroovy().newInstance()

  def loadClassFromGroovy(): Class[TargetDispatcher] = {
    val factory = new ScriptEngineManager
    val engine: ScriptEngine = factory.getEngineByName("groovy")
    assert(engine != null)

    val stream: InputStream = getClass.getResourceAsStream(AppConfig.get(configPath))
    engine.eval(Source.fromInputStream(stream).reader()).asInstanceOf[Class[TargetDispatcher]]
  }
}
