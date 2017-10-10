package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.model.Version


trait Provider[T] {
  def get: T
}


trait TargetResolver extends Provider[TargetResolver] {
  def get: TargetResolver = this

  /**
    * A given target word can express the union of multiple targets; resolving a target consists
    * in computing the list of all the underlying targets (e.g. the European Union
    * COULD be "resolved" in this context as the list of all the names of its member states).
    * An atomic target is a target that cannot be resolved to smaller parts of itself because
    * FOR THE GIVEN PRODUCT, IT WILL NEVER BE POSSIBLE TO DEPLOY TO A SUBSET OF THAT TARGET.
    * If you can choose between Paris and Lyon as a target in France, but nothing finer, then
    * the result of this function `toAtoms` for the input "European Union" CANNOT contain "France"
    * but MUST contain at least "Paris" and "Lyon".
    * For a given atomic target (let's say "Lyon"), this function must return the input itself as
    * only element in the list (i.e. "Lyon" resolves to Iterable("Lyon")).
    *
    * @return the atomic target words to which the input target word is resolved wrt the given product and version.
    */
  def toAtoms(productName: String, productVersion: String, targetWord: String): Iterable[String] =
    Seq(targetWord)
}


trait ParameterFreezer {
  /**
    * @return the execution parameters serialized as they will be provided
    *         to `dispatch` in order to play or replay an execution in a deterministic way,
    *         except that it must be replayable with a subset of the original target (so the targets
    *         should not be included in the frozen parameters).
    *         If the input doesn't make sense (the parameters are incompatible with each other),
    *         it must return an `UnprocessableIntent` error, whose message will be displayed to the end user.
    */
  def freezeParameters(executionKind: String, productName: String, version: Version): String
}


trait TargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer {
  def get: TargetDispatcher = this

  /**
    * @return all the provided target atoms grouped by their dedicated executors
    */
  def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)]
}
