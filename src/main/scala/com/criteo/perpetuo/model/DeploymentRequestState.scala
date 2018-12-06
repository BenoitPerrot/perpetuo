package com.criteo.perpetuo.model

object DeploymentRequestState extends Enumeration {
  type Code = Value

  val notStarted = Value(0, "notStarted")
  val abandoned = Value(1, "abandoned")
  val deployInProgress = Value(2, "deployInProgress")
  val revertInProgress = Value(3, "revertInProgress")
  val deployFailed = Value(4, "deployFailed")
  val revertFailed = Value(5, "revertFailed")
  val deployFlopped = Value(6, "deployFlopped")
  val deployed = Value(7, "deployed")
  val reverted = Value(8, "reverted")
  val paused = Value(9, "paused")
}