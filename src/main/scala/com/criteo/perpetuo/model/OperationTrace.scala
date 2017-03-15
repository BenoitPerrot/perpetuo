package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.Operation.Operation


case class OperationTrace(id: Long,
                          deploymentRequestId: Long,
                          operation: Operation,
                          targetStatus: Status.TargetMap) {
  def partialUpdate(partialTargetStatus: Status.TargetMap): Status.TargetMap = {
    // todo: take into account the precedence of individual target statuses; in JIRA: DREDD-174
    targetStatus ++ partialTargetStatus
  }
}
