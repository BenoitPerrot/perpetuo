package com.criteo.perpetuo.model


case class OperationEffect(operationTrace: OperationTrace,
                           executionTraces: Iterable[ShallowExecutionTrace],
                           targetStatuses: Iterable[TargetStatus])
