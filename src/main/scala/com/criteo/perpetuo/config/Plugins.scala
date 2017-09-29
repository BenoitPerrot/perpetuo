package com.criteo.perpetuo.config

import java.io.File
import java.net.URL
import java.util.logging.Logger

import com.criteo.perpetuo.auth._
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.executors.{DummyInvoker, ExecutorInvoker}
import com.typesafe.config.ConfigException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}
import scala.util.Try


class Plugins(config: RootAppConfig) {
  private val loader = new GroovyScriptLoader(config)

  private def resolve[T](config: AppConfig, typeName: String, groovySupported: Boolean = false)(f: PartialFunction[String, T] = PartialFunction.empty): T = {
    val t: String = try config.get("type") catch {
      case _: ConfigException.Missing => throw new Exception(s"No $typeName is configured, while one is required")
    }
    def instantiate(path: String): T = loader.instantiate(path match {
      case "groovyScriptResource" =>
        getClass.getResource(config.get(t))
      case "groovyScriptFile" =>
        new File(config.get[String](t)).getAbsoluteFile.toURI.toURL
      case unknownType: String =>
        throw new Exception(s"Unknown $typeName configured: $unknownType")
    }).asInstanceOf[T]
    if (groovySupported)
      f.applyOrElse(t, instantiate)
    else
      f(t)
  }

  def invoker(invokerConfig: AppConfig): ExecutorInvoker = {
    resolve(invokerConfig, "invoker") {
      case "dummy" => new DummyInvoker(invokerConfig.get("dummy.name"))
    }
  }

  val dispatcher: TargetDispatcher = {
    val desc = config.under("targetDispatcher")
    resolve(desc, "target dispatcher", groovySupported = true) {
      case t@"singleInvoker" =>
        SingleTargetDispatcher(invoker(desc.under(t)))
    }
  }

  val identityProvider: IdentityProvider =
    Try(config.under("auth.identityProvider")).map { desc =>
      resolve(desc, "type of identity provider") {
        case t@"openAm" =>
          val openAmConfig = desc.under(t)
          new OpenAmIdentityProvider(new URL(openAmConfig.get("authorize.url")), new URL(openAmConfig.get("tokeninfo.url")))
      }
    }.getOrElse(new AnonymousIdentityProvider)

  val permissions: Permissions = {
    // fixme: "Try" only until we get back to a simpler config management system
    Try(config.under("permissions")).map { desc =>
      resolve(desc, "type of permissions", groovySupported = true) {
        case t@"filterUsernames" =>
          new PermissionsByOperationAndUsername(desc.under(t))
      }
    }.getOrElse(new Unrestricted)
  }

  val listener: ListenerPluginWrapper = {
    new ListenerPluginWrapper(
      Try(config.under("engineListener")).toOption.map { desc =>
        resolve[DefaultListenerPlugin](desc, "engine listener", groovySupported = true)()
      }
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
