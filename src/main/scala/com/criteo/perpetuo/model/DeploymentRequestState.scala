package com.criteo.perpetuo.model

object DeploymentRequestState extends Enumeration {
  type Code = Value

  val abandoned = Value(1, "abandoned")
}