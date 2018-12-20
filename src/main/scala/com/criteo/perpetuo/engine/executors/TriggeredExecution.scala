package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.{AppConfig, PluginLoader}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model.ShallowExecutionTrace
import com.typesafe.config.ConfigUtil
import javax.inject.{Inject, Singleton}


/**
  * Reflect an execution that has been triggered on an executor.
  * Any implementation of it must have a constructor taking a href as single parameter.
  * An instantiation must not try to actually reach the executor; each method does.
  * An instantiation must not fail, but must instead consider the execution as unreachable
  * (even for instance if the arguments given to the constructor make no sense).
  */
trait TriggeredExecution {
  val href: String

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

trait TriggeredExecutionFactory {
  def apply(href: String): TriggeredExecution
}

class UncontrollableTriggeredExecution(val href: String) extends TriggeredExecution {
  override val stopper: Option[() => Option[ExecutionState]] = None
}

class UncontrollableTriggeredExecutionFactory extends TriggeredExecutionFactory {
  def apply(href: String): UncontrollableTriggeredExecution =
    new UncontrollableTriggeredExecution(href)
}

/**
  * @return an instance of TriggeredExecution built from an execution trace if possible.
  * @throws RuntimeException if not possible.
  */
@Singleton
class TriggeredExecutionFinder @Inject()(loader: PluginLoader) {
  def apply[T](executionTrace: ShallowExecutionTrace): TriggeredExecution =
    executionTrace.href
      .map { href =>
        val executionFactoryConfig = try
          AppConfig.executorsConfig.getConfig(ConfigUtil.joinPath(executionTrace.executorType, "executionFactory"))
        catch // catch bad configuration path as well as missing configuration
          throwWithCause(s"Could not find an execution configuration for the type `${executionTrace.executorType}`")

        val factory = try
          loader.load[TriggeredExecutionFactory](executionFactoryConfig, "executionFactory")()
        catch // catch any error when dynamically loading the object
          throwWithCause("Could not load the configured executor")

        factory(href)
      }
      .getOrElse(
        throw new RuntimeException(s"No href for execution trace #${executionTrace.id}, thus cannot interact with the actual execution")
      )

  private def throwWithCause(message: String): PartialFunction[Throwable, Nothing] = {
    case cause => throw new RuntimeException(message, cause)
  }
}
