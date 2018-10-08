package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model.Version


class SingleTargetDispatcher(executionTrigger: ExecutionTrigger) extends TargetDispatcher {
  override def freezeParameters(productName: String, version: Version): String = ""

  protected override def dispatch(targetAtoms: TargetAtomSet, frozenParameters: String): Iterable[(ExecutionTrigger, TargetAtomSet)] =
    Map(executionTrigger -> targetAtoms)
}
