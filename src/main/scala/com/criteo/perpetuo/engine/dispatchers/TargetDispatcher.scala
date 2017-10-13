package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.invokers.ExecutorInvoker
import com.criteo.perpetuo.engine.{Provider, Select}
import com.criteo.perpetuo.model.Version


trait ParameterFreezer {
  /**
    * @return the execution parameters serialized as they will be provided
    *         to `dispatch` in order to play or replay an execution in a deterministic way,
    *         except that it must be replayable with a subset of the original target (so the targets
    *         should not be included in the frozen parameters).
    *         If the input doesn't make sense (the parameters are incompatible with each other),
    *         it must return an `UnprocessableIntent` error, whose message will be displayed to the end user.
    */
  def freezeParameters(productName: String, version: Version): String
}


trait TargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer {
  def get: TargetDispatcher = this

  /**
    * @return all the provided target atoms grouped by their dedicated executors
    */
  def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)]
}
