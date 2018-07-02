package com.criteo.perpetuo.model


case class OperationTrace(id: Long,
                          deploymentRequest: DeploymentRequest,
                          kind: Operation.Kind,
                          creator: String,
                          creationDate: java.sql.Timestamp,
                          closingDate: Option[java.sql.Timestamp])
