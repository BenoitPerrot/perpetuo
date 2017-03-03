package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.ExecutionState.ExecutionState


case class ExecutionTrace(id: Long,
                          operationTrace: OperationTrace,
                          logHref: Option[String],
                          state: ExecutionState)
