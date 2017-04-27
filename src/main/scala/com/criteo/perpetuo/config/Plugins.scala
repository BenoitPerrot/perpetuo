package com.criteo.perpetuo.config

import java.io.InputStreamReader
import java.lang.reflect.{InvocationTargetException, Method}
import java.util.logging.Logger
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.dispatchers.TargetDispatcher

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class Plugins(dbBinding: DbBinding, appConfig: BaseAppConfig = AppConfig) {
  lazy val dispatcher: TargetDispatcher = instantiateFromGroovy(AppConfig.get("plugins.dispatcher"))
  lazy val hooks: HooksTrigger = new HooksTrigger(AppConfig.tryGet("plugins.hooks").map(instantiateFromGroovy[Hooks]))


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


trait Plugin {
  /**
    * This logger is the one to use in the Groovy classes, and it should not be overridden
    */
  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
    * Timeout applicable on each plugin method, in seconds; can be overridden in Groovy classes
    */
  val timeout_s: Int = 30
}


abstract class PluginRunner[P <: Plugin](implementation: Option[P], base: P) {
  protected val plugin: P = implementation.getOrElse(base)

  protected def wrap[T](methodName: String, args: Any*): T = {
    val method = plugin.getClass.getMethods.filter(_.getName == methodName).head
    if (method.getDeclaringClass != base.getClass) {
      // there is a specific implementation for this plugin method, let's say it and start it, but time-boxed
      plugin.logger.info(s"$methodName")
      Await.result(Future {
        invoke(method, args: _*)
      }, plugin.timeout_s.seconds)
    }
    else
      invoke(method, args: _*)
  }

  private def invoke[T](method: Method, args: Any*): T =
    try {
      val res = try {
        method.invoke(plugin, args.map(_.asInstanceOf[AnyRef]): _*)
      }
      catch {
        case e: InvocationTargetException => throw e.getCause // redirect to the real error
      }
      res.asInstanceOf[T]
    }
    catch {
      case e: Throwable =>
        // to know which hook is failing, we prefix the trace
        plugin.logger.severe(s"${method.getName} - ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        throw e // fixme: this is only as long as we return something (it's very temporary)
    }
}
