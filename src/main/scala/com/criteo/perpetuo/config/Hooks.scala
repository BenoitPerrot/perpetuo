package com.criteo.perpetuo.config

import java.util.logging.Logger

import com.criteo.perpetuo.model.DeploymentRequest


class Hooks {
  /**
    * Methods that can be overridden as hooks.
    */
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest): Unit = {}

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, immediately: Boolean): Unit = {}


  /** This logger is the one to use in the Groovy hook classes */
  protected val logger: Logger = Logger.getLogger("hooks")


  /**
    * Helpers to catch all errors in a hook and only log them.
    *
    * @param operation the piece of code to be fail-safely executed
    */
  def quietlyLogErrors[T](hookName: String, operation: Runnable): Unit = {
    try {
      operation.run()
    } catch {
      case e: Throwable =>
        val prefix = if (hookName.isEmpty) "" else s"[$hookName] "
        logger.severe(s"$prefix${e.getMessage}\n${e.getStackTrace.mkString("\n")}")
    }
  }

  // we define two methods instead of one having an optional parameter `hookName`, for an easier use in Groovy scripts.
  def quietlyLogErrors(operation: Runnable): Unit = quietlyLogErrors("", operation)
}
