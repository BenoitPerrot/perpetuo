package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine.OperationStatus


case class OperationEffect(operationTrace: OperationTrace,
                           executionTraces: Iterable[ShallowExecutionTrace],
                           targetStatuses: Iterable[TargetStatus]) {
  val state: OperationStatus.Value =
    computeOperationState(operationTrace.closingDate.isEmpty, executionTraces.toStream.map(_.state), targetStatuses.toStream.map(_.code))
}


object computeOperationState {
  // only makes sense if the operation is closed
  def apply(isRunning: Boolean, executionStates: => Iterable[ExecutionState.ExecutionState], statuses: => Iterable[Status.Code]): OperationStatus.Value =
    if (isRunning)
      OperationStatus.inProgress
    else if (statuses.forall(_ == Status.notDone))
      OperationStatus.flopped
    else if (statuses.forall(_ == Status.success) && executionStates.forall(_ == ExecutionState.completed))
      OperationStatus.succeeded
    else
      OperationStatus.failed
}
