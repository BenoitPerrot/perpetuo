package com.criteo.perpetuo.config

import java.io.InputStreamReader
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.dispatchers.TargetDispatcher


class Plugins(appConfig: BaseAppConfig = AppConfig) {
  lazy val dispatcher: TargetDispatcher = instantiateFromGroovy(AppConfig.get("plugins.dispatcher"))
  lazy val hooks: HookMethods = new HookMethods(AppConfig.tryGet("plugins.hooks").map(instantiateFromGroovy[Hooks](_: String)))


  private val factory = new ScriptEngineManager
  private val engine: ScriptEngine = factory.getEngineByName("groovy") // todo? use GroovyScriptEngine
  assert(engine != null)

  def instantiateFromGroovy[T](scriptPath: String): T = {
    val resource = getClass.getResource(scriptPath)
    val cls = engine.eval(new InputStreamReader(resource.openStream())).asInstanceOf[Class[T]]
    val ctor = cls.getConstructor(classOf[RootAppConfig])
    ctor.newInstance(appConfig)
  }
}
