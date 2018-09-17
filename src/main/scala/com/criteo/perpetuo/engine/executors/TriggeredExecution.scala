package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.{AppConfigProvider, PluginLoader}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model.ShallowExecutionTrace
import com.typesafe.config.ConfigUtil
import javax.inject.{Inject, Singleton}


/**
  * Reflect an execution that has been triggered on an executor.
  * Any implementation of it must have a constructor taking a log href as single parameter.
  * An instantiation must not try to actually reach the executor; each method does.
  * An instantiation must not fail, but must instead consider the execution as unreachable
  * (even for instance if the arguments given to the constructor make no sense).
  */
trait TriggeredExecution {
  val logHref: String // todo: find another name for logHref (rename all occurrences) now that it's used to more generally interact with executions

  /**
    * To forcefully stop an execution if supported.
    *
    * @return a function if stopping is supported; the function takes no argument and returns the
    *         eventual state of the execution if it's still not terminated after trying to stop it:
    *         - 'pending' if it is impossible to stop the execution because it is not started yet
    *         - 'running' if the execution could not be stopped and is still seen as running
    *         - 'unreachable' if it is impossible to interact with the execution (permanently lost)
    */
  val stopper: Option[() => Option[ExecutionState]]
}


class UncontrollableTriggeredExecution(val logHref: String) extends TriggeredExecution {
  override val stopper: Option[() => Option[ExecutionState]] = None
}


/**
  * @return an instance of TriggeredExecution built from an execution trace if possible.
  * @throws RuntimeException if not possible.
  */
@Singleton
class TriggeredExecutionFinder @Inject()(loader: PluginLoader) {
  def apply[T](executionTrace: ShallowExecutionTrace): TriggeredExecution =
    executionTrace.href
      .map { logHref =>
        val executionName = logHref match { // todo: look at the executionName in the record instead
          case _ if logHref.contains("/execution/show/") => "rundeck"
          case _ if logHref.contains("/job/") => "jenkins"
          case _ => "unknown"
        }

        val executionConfig = try
          AppConfigProvider.executorsConfig.getConfig(ConfigUtil.joinPath(executionName, "execution"))
        catch // catch bad configuration path as well as missing configuration
          throwWithCause(s"Could not find an execution configuration for the type `$executionName`")

        try
          loader.load[TriggeredExecution](executionConfig, "execution", logHref)
        catch // catch any error when dynamically loading the object
          throwWithCause("Could not load the configured executor")
      }
      .getOrElse(
        throw new RuntimeException(s"No log href for execution trace #${executionTrace.id}, thus cannot interact with the actual execution")
      )

  private def throwWithCause(message: String): PartialFunction[Throwable, Nothing] = {
    case cause => throw new RuntimeException(message, cause)
  }
}
