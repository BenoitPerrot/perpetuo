package com.criteo.perpetuo.model


case class OperationEffect(executionTraces: Iterable[ExecutionTrace], targetStatuses: Iterable[TargetStatus])
