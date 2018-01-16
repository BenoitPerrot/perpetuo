package com.criteo.perpetuo.config

import java.io.File
import java.net.URL
import java.util.logging.Logger

import com.criteo.perpetuo.auth._
import com.criteo.perpetuo.engine.Provider
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.invokers.{DummyInvoker, ExecutorInvoker, RundeckInvoker}
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}


class Plugins(config: Config) {

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  private val loader = new GroovyScriptLoader()

  private def resolve[T <: AnyRef](config: Config, typeName: String, groovySupported: Boolean = false)(f: PartialFunction[String, T] = PartialFunction.empty): T = {
    val t = config.tryGet[String]("type").getOrElse(throw new Exception(s"No $typeName is configured, while one is required"))
    lazy val pluginConfig = config.tryGetConfig("config")
    lazy val stringValue = config.getString(t)

    def instantiate(path: String) = path match {
      case "class" =>
        Plugins.instantiate(Class.forName(stringValue).asInstanceOf[Class[T]], pluginConfig)
      case "groovyScriptResource" =>
        val resource = getClass.getResource(stringValue)
        assert(resource != null, s"Could not find configured resource $stringValue")
        Plugins.instantiate(loader.load(resource), pluginConfig)
      case "groovyScriptFile" =>
        Plugins.instantiate(loader.load(new File(stringValue).getAbsoluteFile.toURI.toURL), pluginConfig)
      case unknownType: String =>
        throw new Exception(s"Unknown $typeName configured: $unknownType")
    }

    if (groovySupported)
      f.applyOrElse(t, instantiate)
    else
      f(t)
  }

  def invoker(invokerConfig: Config): ExecutorInvoker = {
    resolve[ExecutorInvoker](invokerConfig, "invoker") {
      case "dummy" => new DummyInvoker(invokerConfig.getString("dummy.name"))
      case "rundeck" => new RundeckInvoker(
        invokerConfig.getString("rundeck.name"),
        invokerConfig.getString("rundeck.host"),
        invokerConfig.getInt("rundeck.port"),
        invokerConfig.getString("rundeck.token"),
        invokerConfig.getString("rundeck.jobName")
      )
    }
  }

  val resolver: TargetResolver = config
    .tryGetConfig("targetResolver")
    .map { desc =>
      resolve[Provider[TargetResolver]](desc, "target resolver", groovySupported = true)()
    }
    .getOrElse(new TargetResolver {})
    .get

  val dispatcher: TargetDispatcher = config
    .tryGetConfig("targetDispatcher")
    .map { desc =>
      resolve[Provider[TargetDispatcher]](desc, "target dispatcher", groovySupported = true) {
        case t@"singleInvoker" =>
          SingleTargetDispatcher(invoker(desc.getConfig(t)))
      }
    }
    .getOrElse(throw new Exception(s"No target dispatcher is configured, while one is required"))
    .get

  val identityProvider: IdentityProvider =
    config.tryGetConfig("auth.identityProvider").map { desc =>
      resolve[IdentityProvider](desc, "type of identity provider") {
        case t@"openAm" =>
          val openAmConfig = desc.getConfig(t)
          new OpenAmIdentityProvider(new URL(openAmConfig.getString("authorize.url")), new URL(openAmConfig.getString("tokeninfo.url")))
      }
    }.getOrElse(new AnonymousIdentityProvider)

  val permissions: Permissions =
    config.tryGetConfig("permissions").map { desc =>
      resolve[Permissions](desc, "type of permissions", groovySupported = true) {
        case t@"fineGrained" =>
          FineGrainedPermissions.fromConfig(desc.getConfig(t))
      }
    }.getOrElse(new Unrestricted)

  val listeners: Seq[ListenerPluginWrapper] =
    if (config.hasPath("engineListeners"))
      config.getConfigList("engineListeners").asScala.map(desc =>
        new ListenerPluginWrapper(resolve[DefaultListenerPlugin](desc, "engine listener", groovySupported = true)())
      )
    else
      Seq()
}


object Plugins {
  def instantiate[T <: AnyRef](cls: Class[T], optPluginConfig: Option[Config]): T = {
    val instantiationParameters =
      optPluginConfig.map(pluginConfig => Seq(
        Seq(pluginConfig)
      )).getOrElse(Seq()) ++ Seq(
        Seq()
      )
    instantiationParameters
      .view // lazily:
      .flatMap(instantiate(cls, _))
      .headOption
      .getOrElse {
        throw new NoSuchMethodException("Plugins must have at least a constructor taking either a Config or nothing")
      }
  }

  private def instantiate[T <: AnyRef](cls: Class[T], args: Seq[AnyRef]): Option[T] = {
    cls.getConstructors
      .find { c =>
        val types = c.getParameterTypes
        types.length == args.length &&
          types.zip(args).forall { case (t, o) => t.isInstance(o) }
      }
      .map(_.newInstance(args: _*).asInstanceOf[T])
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


abstract class PluginRunner[P <: Plugin](plugin: P, base: P) {
  // fixme: only as long as we need to redirect people who are not in the new workflow to Jira
  protected def wrapTransition[T](toCallOnPlugin: P => T, name: String = null): T = {
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

  protected def wrap(toCallOnPlugin: P => Unit, name: String = null): Unit = {
    val methodName = if (name == null) Thread.currentThread.getStackTrace()(2).getMethodName else name
    try {
      wrapTransition(toCallOnPlugin, methodName)
    }
    catch {
      case _: Throwable =>
    }
  }
}
