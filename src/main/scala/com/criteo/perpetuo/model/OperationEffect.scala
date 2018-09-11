package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine.DeploymentStatus

object OperationEffectState extends Enumeration {
  val inProgress: OperationEffectState.Value = Value("inProgress")
  val flopped: OperationEffectState.Value = Value("flopped")
  val failed: OperationEffectState.Value = Value("failed")
  val succeeded: OperationEffectState.Value = Value("succeeded")

  def from(isRunning: Boolean, executionStates: => Iterable[ExecutionState.ExecutionState], targetStatuses: => Iterable[Status.Code]): OperationEffectState.Value =
    if (isRunning)
      OperationEffectState.inProgress
    else if (targetStatuses.forall(_ == Status.notDone))
      OperationEffectState.flopped
    else if (targetStatuses.forall(_ == Status.success) && executionStates.forall(_ == ExecutionState.completed))
      OperationEffectState.succeeded
    else
      OperationEffectState.failed
}

case class OperationEffect(operationTrace: OperationTrace,
                           deploymentPlanStepIds: Seq[Long],
                           executionTraces: Iterable[ShallowExecutionTrace],
                           targetStatuses: Iterable[TargetStatus]) {
  val state: OperationEffectState.Value =
    OperationEffectState.from(operationTrace.closingDate.isEmpty, executionTraces.toStream.map(_.state), targetStatuses.toStream.map(_.code))
}


object computeOperationState { // fixme: we likely need the deployment status, not the operation one
  // only makes sense if the operation is closed
  def apply(isRunning: Boolean, executionStates: => Iterable[ExecutionState.ExecutionState], statuses: => Iterable[Status.Code]): DeploymentStatus.Value =
    DeploymentStatus.from(OperationEffectState.from(isRunning, executionStates, statuses))
}
