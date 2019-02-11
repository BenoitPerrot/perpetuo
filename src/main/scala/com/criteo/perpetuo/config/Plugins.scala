package com.criteo.perpetuo.config

import java.util.logging.Logger

import com.criteo.perpetuo.auth._
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.engine.{AsyncListener, AsyncPreConditionEvaluator}
import com.google.inject.{Inject, Singleton}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionException, Future, blocking}
import scala.util.{Success, Try}


@Singleton
class Plugins @Inject()(loader: PluginLoader, appConfig: AppConfig) {

  private val config = appConfig.config

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  val resolver: TargetResolver = loader.loadTargetResolver(config.tryGetConfig("targetResolver"))

  val dispatcher: TargetDispatcher = loader.loadTargetDispatcher(config.tryGetConfig("targetDispatcher"))

  val identityProvider: IdentityProvider = loader.loadIdentityProvider(config.tryGetConfig("auth.identityProvider"))

  val permissions: Permissions = loader.loadPermissions(config.tryGetConfig("permissions"))

  val listeners: Seq[AsyncListener] =
    if (config.hasPath("engineListeners"))
      config.getConfigList("engineListeners").map(desc =>
        new ListenerPluginWrapper(loader.load[DefaultListenerPlugin](desc, "engine listener")())
      )
    else
      Seq()

  val preConditionEvaluators: Seq[AsyncPreConditionEvaluator] =
      if (config.hasPath("preConditionEvaluators"))
        config.getConfigList("preConditionEvaluators").map(desc =>
          new AsyncPreConditionWrapper(loader.load[DefaultPreConditionPlugin](desc, "pre-condition evaluator")())
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
  protected implicit def initUnit: () => Unit = () => ()

  protected implicit def initTry: () => Try[Unit] = () => Success(())

  protected def wrap[T](toCallOnPlugin: P => T, name: String = null)(implicit init: () => T): Future[T] =
    try {
      val methodName = if (name == null) Thread.currentThread.getStackTrace()(2).getMethodName else name
      val method = plugin.getClass.getMethods.filter(_.getName == methodName).head
      Future(
        try {
          if (method.getDeclaringClass != base.getClass) {
            // there is a specific implementation for this plugin method, let's start it, but time-boxed
            plugin.logger.fine(methodName)
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
    catch {
      case _: Throwable => Future.successful(init())
    }
}
