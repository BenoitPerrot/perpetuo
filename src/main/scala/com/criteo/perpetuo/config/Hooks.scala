package com.criteo.perpetuo.config

import java.lang.reflect.InvocationTargetException
import java.util.logging.Logger

import com.criteo.perpetuo.model.DeploymentRequest

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


// fixme: temporarily parametrized
trait ImplementableHooks[T] {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): T

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean)
}


class Hooks extends ImplementableHooks[String] {
  /**
    * Methods that can be overridden as hooks.
    */
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit = {}


  /**
    * This logger is the one to use in the Groovy hook classes, and it should not be overridden
    */
  val logger: Logger = Logger.getLogger("hooks")

  /**
    * Timeout applicable on each hook, in seconds; can be overridden in Groovy hook classes
    */
  val timeout_s: Int = 30
}


private[config] class HooksTrigger(implementation: Option[Hooks]) extends ImplementableHooks[Future[String]] {
  private val hooks: Hooks = implementation.getOrElse(new Hooks)

  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): Future[String] =
    wrap("onDeploymentRequestCreated", deploymentRequest, immediateStart)

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit =
    wrap("onDeploymentRequestStarted", deploymentRequest, startedExecutions, failedToStart, immediately)

  private def wrap(methodName: String, args: Any*): Future[String] = {
    val method = hooks.getClass.getMethods.filter(_.getName == methodName).head
    if (method.getDeclaringClass != classOf[Hooks]) {
      // there is a specific implementation for this hook, let's start it in a background thread after logging its name
      hooks.logger.info(s"$methodName")
      Future {
        var ret: String = null
        try {
          ret = Await.result(
            Future {
              method.invoke(hooks, args.map(_.asInstanceOf[AnyRef]): _*)
            },
            hooks.timeout_s.seconds
          ).asInstanceOf[String]
        }
        catch {
          case e: InvocationTargetException => logException(e.getCause, methodName)
          case e: Throwable => logException(e, methodName)
        }
        ret
      }
    }
    else
      Future.successful(null)
  }

  private def logException(exc: Throwable, methodName: String) =
    hooks.logger.severe(s"$methodName - ${exc.getMessage}\n${exc.getStackTrace.mkString("\n")}")
}
