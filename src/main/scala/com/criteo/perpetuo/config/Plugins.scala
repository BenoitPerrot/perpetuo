package com.criteo.perpetuo.config

import java.util.logging.Logger

import com.criteo.perpetuo.auth._
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.invokers.{DummyInvoker, ExecutorInvoker, RundeckInvoker}
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.engine.{AsyncListener, Provider}
import com.google.inject.{Inject, Singleton}
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}

@Singleton
class Plugins @Inject()(loader: PluginLoader) {

  private val config = AppConfigProvider.config

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  def invoker(invokerConfig: Config): ExecutorInvoker = {
    loader.load[ExecutorInvoker](invokerConfig, "invoker") {
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
      loader.load[Provider[TargetResolver]](desc, "target resolver")()
    }
    .getOrElse(new TargetResolver {})
    .get

  val dispatcher: TargetDispatcher = config
    .tryGetConfig("targetDispatcher")
    .map { desc =>
      loader.load[Provider[TargetDispatcher]](desc, "target dispatcher") {
        case t@"singleInvoker" =>
          SingleTargetDispatcher(invoker(desc.getConfig(t)))
      }
    }
    .getOrElse(throw new Exception(s"No target dispatcher is configured, while one is required"))
    .get

  val identityProvider: IdentityProvider =
    config.tryGetConfig("auth.identityProvider").map { desc =>
      loader.load[IdentityProvider](desc, "type of identity provider") {
        case t@"openAm" =>
          OpenAmIdentityProvider.fromConfig(desc.getConfig(t))
      }
    }.getOrElse(AnonymousIdentityProvider)

  val permissions: Permissions =
    config.tryGetConfig("permissions").map { desc =>
      loader.load[Permissions](desc, "type of permissions") {
        case t@"fineGrained" =>
          FineGrainedPermissions.fromConfig(desc.getConfig(t))
      }
    }.getOrElse(Unrestricted)

  val listeners: Seq[AsyncListener] =
    if (config.hasPath("engineListeners"))
      config.getConfigList("engineListeners").asScala.map(desc =>
        new ListenerPluginWrapper(loader.load[DefaultListenerPlugin](desc, "engine listener")())
      )
    else
      Seq()
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
  protected def wrapTransition[T](toCallOnPlugin: P => T, name: String = null): Future[T] = {
    val methodName = if (name == null) Thread.currentThread.getStackTrace()(2).getMethodName else name
    val method = plugin.getClass.getMethods.filter(_.getName == methodName).head
    Future(
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
    )
  }

  protected def wrap(toCallOnPlugin: P => Unit, name: String = null): Future[Unit] = {
    val methodName = if (name == null) Thread.currentThread.getStackTrace()(2).getMethodName else name
    try {
      wrapTransition(toCallOnPlugin, methodName)
    }
    catch {
      case _: Throwable => Future.successful()
    }
  }
}
