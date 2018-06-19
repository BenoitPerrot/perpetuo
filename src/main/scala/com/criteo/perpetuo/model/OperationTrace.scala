package com.criteo.perpetuo.model


trait OperationTrace {
  val id: Long
  val deploymentRequestId: Long
  val kind: Operation.Kind
  val creator: String
  val creationDate: java.sql.Timestamp
  val closingDate: Option[java.sql.Timestamp]
}


case class ShallowOperationTrace(id: Long,
                                 deploymentRequestId: Long,
                                 kind: Operation.Kind,
                                 creator: String,
                                 creationDate: java.sql.Timestamp,
                                 closingDate: Option[java.sql.Timestamp]) extends OperationTrace {
  def toDeepOperationTrace(deploymentRequest: DeploymentRequest): DeepOperationTrace ={
    assert(deploymentRequest.id == deploymentRequestId)
    DeepOperationTrace(id, deploymentRequest, kind, creator, creationDate, closingDate)
  }
}


case class DeepOperationTrace(id: Long,
                              deploymentRequest: DeploymentRequest,
                              kind: Operation.Kind,
                              creator: String,
                              creationDate: java.sql.Timestamp,
                              closingDate: Option[java.sql.Timestamp]) extends OperationTrace {
  val deploymentRequestId: Long = deploymentRequest.id
}
