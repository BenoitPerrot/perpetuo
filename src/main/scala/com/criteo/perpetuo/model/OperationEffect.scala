package com.criteo.perpetuo.model


case class OperationEffect(operationTrace: ShallowOperationTrace,
                           executionTraces: Iterable[ExecutionTrace],
                           targetStatuses: Iterable[TargetStatus])
