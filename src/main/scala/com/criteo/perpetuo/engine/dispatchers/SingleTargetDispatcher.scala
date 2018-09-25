package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model.{TargetExpr, Version}


class SingleTargetDispatcher(executionTrigger: ExecutionTrigger) extends TargetDispatcher {
  override def freezeParameters(productName: String, version: Version): String = ""

  protected override def dispatch(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] =
    Map(executionTrigger -> targetExpr)
}
