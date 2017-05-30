package com.criteo.perpetuo.config

import java.io.InputStreamReader
import java.lang.reflect.{InvocationTargetException, Method}
import java.util
import java.util.logging.Logger
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.dispatchers.TargetDispatcher

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class Plugins(dbBinding: DbBinding, appConfig: BaseAppConfig = AppConfig) {
  private val engine: ScriptEngine = new ScriptEngineManager().getEngineByName("groovy") // todo? use GroovyScriptEngine
  assert(engine != null)

  def instantiateFromGroovy(scriptPath: String): AnyRef = {
    val resource = getClass.getResource(scriptPath)
    val cls = engine.eval(new InputStreamReader(resource.openStream())).asInstanceOf[Class[AnyRef]]
    Seq( // supported instantiation parameters:
      Seq(dbBinding, appConfig),
      Seq()
    )
      .view // lazily:
      .flatMap(instantiate(cls, _))
      .headOption
      .getOrElse {
        throw new NoSuchMethodException("Every plugin must implement either a constructor taking a DbBinding and a RootAppConfig as parameters or a default constructor")
      }
  }

  private def instantiate(cls: Class[AnyRef], args: Seq[AnyRef]): Option[AnyRef] = {
    cls.getConstructors
      .find { c =>
        val types = c.getParameterTypes
        types.length == args.length &&
          types.zip(args).forall { case (t, o) => t.isInstance(o) }
      }
      .map(_.newInstance(args: _*).asInstanceOf[AnyRef])
  }

  // load the plugins in the declared order, because one plugin might use what has been defined by another
  private val tempInstances: mutable.Map[String, AnyRef] = mutable.Map(AppConfig
    .get[util.ArrayList[util.HashMap[String, String]]]("plugins")
    .asScala.map(_.asScala)
    .map(obj => obj("name") -> instantiateFromGroovy(obj("path"))): _*)


  val dispatcher: TargetDispatcher = tempInstances.remove("dispatcher").get.asInstanceOf[TargetDispatcher]
  val externalData: ExternalDataGetter = new ExternalDataGetter(tempInstances.remove("externalData").map(_.asInstanceOf[ExternalData]))
  val hooks: HooksTrigger = new HooksTrigger(tempInstances.remove("hooks").map(_.asInstanceOf[Hooks]))
  assert(tempInstances.isEmpty, s"Unused plugin(s): ${tempInstances.keys.mkString(", ")}")
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
        invoke[T](method, args: _*)
      }, plugin.timeout_s.seconds)
    }
    else
      invoke[T](method, args: _*)
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
