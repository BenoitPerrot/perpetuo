package com.criteo.perpetuo.config

import java.util.logging.Logger

import com.criteo.perpetuo.auth.{Unrestricted, Permissions, PermissionsByOperationAndUsername}
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.executors.{DummyInvoker, ExecutorInvoker}
import com.typesafe.config.ConfigException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}
import scala.util.Try


class Plugins(appConfig: RootAppConfig = AppConfig) {
  private val loader = new GroovyScriptLoader(appConfig)

  private def resolve[T](config: AppConfig, typeName: String, groovySupported: Boolean = false)(f: PartialFunction[String, T] = PartialFunction.empty): T = {
    val t: String = try config.get("type") catch {
      case _: ConfigException.Missing => throw new Exception(s"No $typeName is configured, while one is required")
    }
    f.applyOrElse(t, (_: String) match {
      case t@"groovyScript" =>
        loader.instantiate(config.get(t)).asInstanceOf[T]
      case unknownType: String =>
        throw new Exception(s"Unknown $typeName configured: $unknownType")
    })
  }

  def invoker(invokerConfig: AppConfig): ExecutorInvoker = {
    resolve(invokerConfig, "invoker") {
      case "dummy" => new DummyInvoker(invokerConfig.get("dummy.name"))
    }
  }

  val dispatcher: TargetDispatcher = {
    val desc = AppConfig.under("targetDispatcher")
    resolve(desc, "target dispatcher", groovySupported = true) {
      case t@"singleInvoker" =>
        SingleTargetDispatcher(invoker(desc.under(t)))
    }
  }

  val permissions: Permissions = {
    // fixme: "Try" only until we get back to a simpler config management system
    Try(AppConfig.under("permissions")).map { desc =>
      resolve(desc, "type of permissions", groovySupported = true) {
        case t@"filterUsernames" =>
          new PermissionsByOperationAndUsername(desc.under(t))
      }
    }.getOrElse(new Unrestricted)
  }

  val listener: ListenerPluginWrapper = {
    new ListenerPluginWrapper(
      Try(AppConfig.under("engineListener")).map { desc =>
        resolve(desc, "engine listener", groovySupported = true)()
      }.toOption
    )
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
