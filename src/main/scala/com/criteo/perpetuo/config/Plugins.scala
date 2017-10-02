package com.criteo.perpetuo.config

import java.io.File
import java.net.URL
import java.util.logging.Logger

import com.criteo.perpetuo.auth._
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.executors.{DummyInvoker, ExecutorInvoker, RundeckInvoker}
import com.typesafe.config.{Config, ConfigException}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}


class Plugins(config: Config) {
  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  private val loader = new GroovyScriptLoader(config)

  private def resolve[T](config: Config, typeName: String, groovySupported: Boolean = false)(f: PartialFunction[String, T] = PartialFunction.empty): T = {
    val t: String = config.tryGet[String]("type").getOrElse(throw new Exception(s"No $typeName is configured, while one is required"))
    def instantiate(path: String): T = loader.instantiate(path match {
      case "groovyScriptResource" =>
        getClass.getResource(config.getString(t))
      case "groovyScriptFile" =>
        new File(config.getString(t)).getAbsoluteFile.toURI.toURL
      case unknownType: String =>
        throw new Exception(s"Unknown $typeName configured: $unknownType")
    }).asInstanceOf[T]
    if (groovySupported)
      f.applyOrElse(t, instantiate)
    else
      f(t)
  }

  def invoker(invokerConfig: Config): ExecutorInvoker = {
    resolve(invokerConfig, "invoker") {
      case "dummy" => new DummyInvoker(invokerConfig.getString("dummy.name"))
      case "rundeck" => new RundeckInvoker(
        invokerConfig.get("rundeck.name"),
        invokerConfig.get("rundeck.host"),
        invokerConfig.get("rundeck.port"),
        invokerConfig.get("rundeck.token"),
        invokerConfig.get("rundeck.jobName")
      )
    }
  }

  val dispatcher: TargetDispatcher =
    config.tryGetConfig("targetDispatcher").map { desc =>
      resolve(desc, "target dispatcher", groovySupported = true) {
        case t@"singleInvoker" =>
          SingleTargetDispatcher(invoker(desc.getConfig(t)))
      }
    }.getOrElse(throw new Exception(s"No target dispatcher is configured, while one is required"))

  val identityProvider: IdentityProvider =
    config.tryGetConfig("auth.identityProvider").map { desc =>
      resolve(desc, "type of identity provider") {
        case t@"openAm" =>
          val openAmConfig = desc.getConfig(t)
          new OpenAmIdentityProvider(new URL(openAmConfig.getString("authorize.url")), new URL(openAmConfig.getString("tokeninfo.url")))
      }
    }.getOrElse(new AnonymousIdentityProvider)

  val permissions: Permissions =
    config.tryGetConfig("permissions").map { desc =>
      resolve(desc, "type of permissions", groovySupported = true) {
        case t@"filterUsernames" =>
          new PermissionsByOperationAndUsername(desc.getConfig(t))
      }
    }.getOrElse(new Unrestricted)

  val listener: ListenerPluginWrapper =
    new ListenerPluginWrapper(
      config.tryGetConfig("engineListener").map { desc =>
        resolve[DefaultListenerPlugin](desc, "engine listener", groovySupported = true)()
      }
    )
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
