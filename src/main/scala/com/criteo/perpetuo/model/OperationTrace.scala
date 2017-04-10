package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.Operation.Operation
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}


case class OperationTrace(id: Long,
                          @JsonIgnore deploymentRequestId: Long,
                          @JsonProperty("type") operation: Operation,
                          targetStatus: Status.TargetMap) {
  def partialUpdate(partialTargetStatus: Status.TargetMap): Status.TargetMap = {
    // todo: take into account the precedence of individual target statuses; in JIRA: DREDD-174
    targetStatus ++ partialTargetStatus
  }
}
