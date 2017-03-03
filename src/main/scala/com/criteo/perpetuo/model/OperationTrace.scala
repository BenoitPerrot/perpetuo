package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.Operation.Operation


case class OperationTrace(id: Long,
                          deploymentRequestId: Long,
                          operation: Operation,
                          targetStatus: TargetStatus.MapType) {
  def partialUpdate(partialTargetStatus: TargetStatus.MapType): TargetStatus.MapType = {
    // todo: take into account the precedence of individual target statuses; in JIRA: DREDD-174
    targetStatus ++ partialTargetStatus
  }
}
