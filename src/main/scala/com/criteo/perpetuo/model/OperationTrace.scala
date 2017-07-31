package com.criteo.perpetuo.model

import com.criteo.perpetuo.model.Operation.Operation
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}


trait OperationTrace {
  val id: Long
  val deploymentRequestId: Long
  val operation: Operation
  val creator: String
  val creationDate: java.sql.Timestamp
}


case class ShallowOperationTrace(id: Long,
                                 @JsonIgnore deploymentRequestId: Long,
                                 @JsonProperty("type") operation: Operation,
                                 creator: String,
                                 creationDate: java.sql.Timestamp,
                                 targetStatus: Status.TargetMap) extends OperationTrace {
  def partialUpdate(partialTargetStatus: Status.TargetMap): Status.TargetMap = {
    // todo: take into account the precedence of individual target statuses; in JIRA: DREDD-174
    targetStatus ++ partialTargetStatus
  }
}


case class DeepOperationTrace(id: Long,
                              deploymentRequest: DeploymentRequest,
                              operation: Operation,
                              creator: String,
                              creationDate: java.sql.Timestamp) extends OperationTrace {
  val deploymentRequestId: Long = deploymentRequest.id
}
