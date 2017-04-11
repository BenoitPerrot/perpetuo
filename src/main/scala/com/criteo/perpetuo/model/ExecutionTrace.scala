package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}


@JsonInclude(Include.ALWAYS)
case class ExecutionTrace(id: Long,
                          @JsonIgnore operationTrace: OperationTrace,
                          logHref: Option[String],
                          state: ExecutionState)
