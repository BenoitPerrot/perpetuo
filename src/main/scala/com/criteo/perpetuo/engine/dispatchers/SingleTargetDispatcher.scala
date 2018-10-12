package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model.{TargetAtom, Version}


class SingleTargetDispatcher(executionTrigger: ExecutionTrigger) extends TargetDispatcher {
  override def freezeParameters(productName: String, version: Version): String = ""

  protected override def dispatch(targetAtoms: Set[TargetAtom], frozenParameters: String): Iterable[(ExecutionTrigger, Set[TargetAtom])] =
    Map(executionTrigger -> targetAtoms)
}
