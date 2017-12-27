package com.criteo.perpetuo.model


case class OperationEffect(operationTrace: ShallowOperationTrace,
                           executionTraces: Iterable[ShallowExecutionTrace],
                           targetStatuses: Iterable[TargetStatus])
