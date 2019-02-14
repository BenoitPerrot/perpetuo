package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model._

sealed trait DeploymentState {
  val deploymentRequest: DeploymentRequest
  val deploymentPlanSteps: Seq[DeploymentPlanStep]
  val effects: Seq[OperationEffect]
  val outdatedBy: Option[Long]

  def isOutdated: Boolean = outdatedBy.nonEmpty

  override def toString: String = {
    val chars = getClass.getSimpleName.toCharArray
    chars(0) = chars(0).toLower
    new String(chars)
  }
}

sealed trait InProgressState extends DeploymentState {
  val scope: OperationEffect
}

sealed trait RevertibleState extends DeploymentState {
  val revertScope: Seq[DeploymentPlanStep]
}

final case class NotStarted(deploymentRequest: DeploymentRequest,
                            deploymentPlanSteps: Seq[DeploymentPlanStep],
                            effects: Seq[OperationEffect],
                            toDo: DeploymentPlanStep,
                            outdatedBy: Option[Long])
  extends DeploymentState

final case class RevertInProgress(deploymentRequest: DeploymentRequest,
                                  deploymentPlanSteps: Seq[DeploymentPlanStep],
                                  effects: Seq[OperationEffect],
                                  scope: OperationEffect,
                                  outdatedBy: Option[Long])
  extends InProgressState

final case class RevertFailed(deploymentRequest: DeploymentRequest,
                              deploymentPlanSteps: Seq[DeploymentPlanStep],
                              effects: Seq[OperationEffect],
                              revertScope: Seq[DeploymentPlanStep],
                              outdatedBy: Option[Long])
  extends RevertibleState

final case class Reverted(deploymentRequest: DeploymentRequest,
                          deploymentPlanSteps: Seq[DeploymentPlanStep],
                          effects: Seq[OperationEffect],
                          outdatedBy: Option[Long])
  extends DeploymentState

final case class DeployFlopped(deploymentRequest: DeploymentRequest,
                               deploymentPlanSteps: Seq[DeploymentPlanStep],
                               effects: Seq[OperationEffect],
                               step: DeploymentPlanStep,
                               outdatedBy: Option[Long])
  extends DeploymentState

final case class DeployInProgress(deploymentRequest: DeploymentRequest,
                                  deploymentPlanSteps: Seq[DeploymentPlanStep],
                                  effects: Seq[OperationEffect],
                                  scope: OperationEffect,
                                  outdatedBy: Option[Long])
  extends InProgressState

final case class DeployFailed(deploymentRequest: DeploymentRequest,
                              deploymentPlanSteps: Seq[DeploymentPlanStep],
                              effects: Seq[OperationEffect],
                              step: DeploymentPlanStep,
                              revertScope: Seq[DeploymentPlanStep],
                              outdatedBy: Option[Long])
  extends RevertibleState

final case class Paused(deploymentRequest: DeploymentRequest,
                        deploymentPlanSteps: Seq[DeploymentPlanStep],
                        effects: Seq[OperationEffect],
                        toDo: DeploymentPlanStep,
                        lastDone: DeploymentPlanStep,
                        revertScope: Seq[DeploymentPlanStep],
                        outdatedBy: Option[Long])
  extends RevertibleState

final case class Deployed(deploymentRequest: DeploymentRequest,
                          deploymentPlanSteps: Seq[DeploymentPlanStep],
                          effects: Seq[OperationEffect],
                          revertScope: Seq[DeploymentPlanStep],
                          outdatedBy: Option[Long])
  extends RevertibleState

final case class Abandoned(deploymentRequest: DeploymentRequest,
                           deploymentPlanSteps: Seq[DeploymentPlanStep],
                           effects: Seq[OperationEffect],
                           outdatedBy: Option[Long])
  extends DeploymentState

final case class Superseded(deploymentRequest: DeploymentRequest,
                            deploymentPlanSteps: Seq[DeploymentPlanStep],
                            effects: Seq[OperationEffect],
                            outdatedBy: Option[Long])
  extends DeploymentState
