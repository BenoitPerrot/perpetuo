package com.criteo.perpetuo.model


case class OperationEffect(operationTrace: OperationTrace,
                           executionTraces: Iterable[ExecutionTrace],
                           targetStatuses: Iterable[TargetStatus])
