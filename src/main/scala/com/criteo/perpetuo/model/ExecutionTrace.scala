package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include


@JsonInclude(Include.ALWAYS)
case class ShallowExecutionTrace(id: Long,
                                 href: Option[String],
                                 state: ExecutionState,
                                 detail: String)


case class ExecutionTraceBranch(id: Long,
                                executionId: Long,
                                operationTrace: OperationTrace,
                                href: Option[String],
                                state: ExecutionState,
                                detail: String)
