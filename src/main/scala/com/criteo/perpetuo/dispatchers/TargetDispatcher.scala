package com.criteo.perpetuo.dispatchers

import java.io.{File, FileReader}
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.app.AppConfig
import com.criteo.perpetuo.executors.ExecutorInvoker


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

    val script = new File(AppConfig.get[String](configPath))
    engine.eval(new FileReader(script)).asInstanceOf[Class[TargetDispatcher]]
  }
}
