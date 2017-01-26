package com.criteo.perpetuo.model

import Operation.Operation

case class OperationTrace(id: Option[Long],
                          deploymentRequestId: Long,
                          operation: Operation,
                          targetStatus: TargetStatus.MapType = Map())
