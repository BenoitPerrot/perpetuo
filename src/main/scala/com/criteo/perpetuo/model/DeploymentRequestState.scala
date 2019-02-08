package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine._

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
  val superseded = Value(10, "superseded")

  def from(state: DeploymentState): DeploymentRequestState.Code =
    state match {
      case _: Abandoned => abandoned
      case _: Deployed => deployed
      case _: DeployInProgress => deployInProgress
      case _: DeployFailed => deployFailed
      case _: DeployFlopped => deployFlopped
      case _: Reverted => reverted
      case _: RevertInProgress => revertInProgress
      case _: RevertFailed => revertFailed
      case _: NotStarted => notStarted
      case _: Paused => paused
      case _: Superseded => superseded
    }
}