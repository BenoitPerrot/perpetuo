package com.criteo.perpetuo.config

import java.io.InputStreamReader
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.dispatchers.TargetDispatcher


class Plugins(dbBinding: DbBinding, appConfig: BaseAppConfig = AppConfig) {
  lazy val dispatcher: TargetDispatcher = instantiateFromGroovy(AppConfig.get("plugins.dispatcher"))
  lazy val hooks: HooksTrigger = new HooksTrigger(AppConfig.tryGet("plugins.hooks").map(instantiateFromGroovy[Hooks](_: String)))


  private val factory = new ScriptEngineManager
  private val engine: ScriptEngine = factory.getEngineByName("groovy") // todo? use GroovyScriptEngine
  assert(engine != null)

  def instantiateFromGroovy[T](scriptPath: String): T = {
    val resource = getClass.getResource(scriptPath)
    val cls = engine.eval(new InputStreamReader(resource.openStream())).asInstanceOf[Class[T]]
    val ctor = cls.getConstructor(classOf[DbBinding], classOf[RootAppConfig])
    ctor.newInstance(dbBinding, appConfig)
  }
}
