package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine.DeploymentStatus


case class OperationEffect(operationTrace: OperationTrace,
                           executionTraces: Iterable[ShallowExecutionTrace],
                           targetStatuses: Iterable[TargetStatus]) {
  val state: DeploymentStatus.Value =
    computeOperationState(operationTrace.closingDate.isEmpty, executionTraces.toStream.map(_.state), targetStatuses.toStream.map(_.code))
}


object computeOperationState { // fixme: we likely need the deployment status, not the operation one
  // only makes sense if the operation is closed
  def apply(isRunning: Boolean, executionStates: => Iterable[ExecutionState.ExecutionState], statuses: => Iterable[Status.Code]): DeploymentStatus.Value =
    if (isRunning)
      DeploymentStatus.inProgress
    else if (statuses.forall(_ == Status.notDone))
      DeploymentStatus.flopped
    else if (statuses.forall(_ == Status.success) && executionStates.forall(_ == ExecutionState.completed))
      DeploymentStatus.succeeded
    else
      DeploymentStatus.failed
}
