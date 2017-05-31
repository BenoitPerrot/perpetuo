package com.criteo.perpetuo.config

import java.io.InputStreamReader
import java.util.logging.Logger
import javax.script.{ScriptEngine, ScriptEngineManager}

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.dispatchers.TargetDispatcher

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect._


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
  private var tempInstances: Seq[AnyRef] =
    AppConfig
      .get[java.util.ArrayList[String]]("plugins").asScala
      .map(instantiateFromGroovy)

  private def extractInstance[T: ClassTag]: Option[T] = {
    val (selected, rejected) = tempInstances.partition(classTag[T].runtimeClass.isInstance)
    assert(selected.length <= 1, s"Expected to find at most one instance of ${classTag[T].runtimeClass.getName}, found instances of: ${selected.map(_.getClass.getName).mkString(", ")}")
    tempInstances = rejected
    selected.headOption.map(_.asInstanceOf[T])
  }


  val dispatcher: TargetDispatcher = extractInstance[TargetDispatcher].get
  val externalData: ExternalDataGetter = new ExternalDataGetter(extractInstance[ExternalData])
  val hooks: HooksTrigger = new HooksTrigger(extractInstance[Hooks])
  assert(tempInstances.isEmpty, s"Unused plugin(s): ${tempInstances.map(_.getClass.getName).mkString(", ")}")
}


trait Plugin {
  /**
    * This logger is the one to use in the Groovy classes, and it should not be overridden
    */
  val logger: Logger = Logger.getLogger(getClass.getName)

  /**
    * Timeout applicable on each plugin method, in seconds; can be overridden in Groovy classes
    */
  val timeout_s: Int
}


abstract class PluginRunner[P <: Plugin](implementation: Option[P], base: P) {
  protected val plugin: P = implementation.getOrElse(base)

  protected def wrap[T](toCallOnPlugin: P => T, name: String = null): T = {
    val methodName = if (name == null) Thread.currentThread.getStackTrace()(2).getMethodName else name
    val method = plugin.getClass.getMethods.filter(_.getName == methodName).head
    try {
      if (method.getDeclaringClass != base.getClass) {
        // there is a specific implementation for this plugin method, let's say it and start it, but time-boxed
        plugin.logger.info(methodName)
        Await.result(Future {
          toCallOnPlugin(plugin)
        }, plugin.timeout_s.seconds)
      }
      else
        toCallOnPlugin(plugin)
    }
    catch {
      case e: Throwable =>
        // to know which hook is failing, we prefix the trace
        plugin.logger.severe(s"$methodName - ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        throw e
    }
  }
}
