package com.criteo.perpetuo.model

import ExecutionState.ExecutionState

case class ExecutionTrace(id: Option[Long],
                          operationTraceId: Long,
                          uuid: Option[String] = None,
                          state: ExecutionState = ExecutionState.pending)
