package com.criteo.perpetuo.config

import java.util.logging.Logger

import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.executors.{DummyInvoker, ExecutorInvoker}
import com.typesafe.config.ConfigException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}
import scala.reflect._


class Plugins(appConfig: RootAppConfig = AppConfig) {
  private val loader = new GroovyScriptLoader(appConfig)

  // load the plugins in the declared order, because one plugin might use what has been defined by another
  private var tempInstances: Seq[AnyRef] =
    AppConfig
      .tryGet[Seq[String]]("plugins")
      .map(_.map(loader.instantiate))
      .getOrElse(Seq())

  private def extractInstance[T: ClassTag]: Option[T] = {
    val (selected, rejected) = tempInstances.partition(classTag[T].runtimeClass.isInstance)
    assert(selected.length <= 1, s"Expected to find at most one instance of ${classTag[T].runtimeClass.getName}, found instances of: ${selected.map(_.getClass.getName).mkString(", ")}")
    tempInstances = rejected
    selected.headOption.map(_.asInstanceOf[T])
  }

  def invoker(invokerConfig: AppConfig): ExecutorInvoker =
    try {
      invokerConfig.get[String]("type") match {
        case "dummy" => new DummyInvoker(invokerConfig.get[String]("dummy.name"))
        case unknownType => throw new Exception(s"Unknown invoker configured: $unknownType")
      }
    } catch {
      case _: ConfigException.Missing => throw new Exception("No invoker is configured, while one is required")
    }

  val dispatcher: TargetDispatcher = {
    val desc = AppConfig.under("targetDispatcher")
    try {
      desc.get[String]("type") match {
        case t@"groovyScript" =>
          loader.instantiate(desc.get(t)).asInstanceOf[TargetDispatcher]
        case t@"singleInvoker" =>
          SingleTargetDispatcher(invoker(desc.under(t)))
        case unknownType => throw new Exception(s"Unknown target dispatcher configured: $unknownType")
      }
    } catch {
      case _: ConfigException.Missing => throw new Exception("No target dispatcher is configured, while one is required")
    }
  }

  val externalData: ExternalDataGetter = new ExternalDataGetter(extractInstance[ExternalData])
  val listener: ListenerPluginWrapper = new ListenerPluginWrapper(extractInstance[DefaultListenerPlugin])
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
        try {
          Await.result(Future {
            blocking {
              toCallOnPlugin(plugin)
            }
          }, plugin.timeout_s.seconds)
        }
        catch {
          case e: ExecutionException => throw e.getCause
        }
      }
      else
        toCallOnPlugin(plugin)
    }
    catch {
      case e: Throwable =>
        // to know which method is failing, prefix the trace
        plugin.logger.severe(s"$methodName - ${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
        throw e
    }
  }
}
