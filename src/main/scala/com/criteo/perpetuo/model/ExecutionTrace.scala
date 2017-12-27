package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include


@JsonInclude(Include.ALWAYS)
case class ShallowExecutionTrace(id: Long,
                                 logHref: Option[String],
                                 state: ExecutionState,
                                 detail: String)


case class DeepExecutionTrace(id: Long,
                              executionId: Long,
                              operationTrace: ShallowOperationTrace,
                              logHref: Option[String],
                              state: ExecutionState,
                              detail: String)
