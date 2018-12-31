package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model._

trait DeploymentState {
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

trait InProgressState extends DeploymentState {
  val scope: OperationEffect
}

trait RevertibleState extends DeploymentState {
  val revertScope: Seq[DeploymentPlanStep]
}

case class NotStarted(deploymentRequest: DeploymentRequest,
                      deploymentPlanSteps: Seq[DeploymentPlanStep],
                      effects: Seq[OperationEffect],
                      toDo: DeploymentPlanStep,
                      outdatedBy: Option[Long])
  extends DeploymentState

case class RevertInProgress(deploymentRequest: DeploymentRequest,
                            deploymentPlanSteps: Seq[DeploymentPlanStep],
                            effects: Seq[OperationEffect],
                            scope: OperationEffect,
                            outdatedBy: Option[Long])
  extends InProgressState

case class RevertFailed(deploymentRequest: DeploymentRequest,
                        deploymentPlanSteps: Seq[DeploymentPlanStep],
                        effects: Seq[OperationEffect],
                        revertScope: Seq[DeploymentPlanStep],
                        outdatedBy: Option[Long])
  extends RevertibleState

case class Reverted(deploymentRequest: DeploymentRequest,
                    deploymentPlanSteps: Seq[DeploymentPlanStep],
                    effects: Seq[OperationEffect],
                    outdatedBy: Option[Long])
  extends DeploymentState

case class DeployFlopped(deploymentRequest: DeploymentRequest,
                         deploymentPlanSteps: Seq[DeploymentPlanStep],
                         effects: Seq[OperationEffect],
                         step: DeploymentPlanStep,
                         outdatedBy: Option[Long])
  extends DeploymentState

case class DeployInProgress(deploymentRequest: DeploymentRequest,
                            deploymentPlanSteps: Seq[DeploymentPlanStep],
                            effects: Seq[OperationEffect],
                            scope: OperationEffect,
                            outdatedBy: Option[Long])
  extends InProgressState

case class DeployFailed(deploymentRequest: DeploymentRequest,
                        deploymentPlanSteps: Seq[DeploymentPlanStep],
                        effects: Seq[OperationEffect],
                        step: DeploymentPlanStep,
                        revertScope: Seq[DeploymentPlanStep],
                        outdatedBy: Option[Long])
  extends RevertibleState

case class Paused(deploymentRequest: DeploymentRequest,
                  deploymentPlanSteps: Seq[DeploymentPlanStep],
                  effects: Seq[OperationEffect],
                  toDo: DeploymentPlanStep,
                  lastDone: DeploymentPlanStep,
                  revertScope: Seq[DeploymentPlanStep],
                  outdatedBy: Option[Long])
  extends RevertibleState

case class Deployed(deploymentRequest: DeploymentRequest,
                    deploymentPlanSteps: Seq[DeploymentPlanStep],
                    effects: Seq[OperationEffect],
                    revertScope: Seq[DeploymentPlanStep],
                    outdatedBy: Option[Long])
  extends RevertibleState

case class Abandoned(deploymentRequest: DeploymentRequest,
                     deploymentPlanSteps: Seq[DeploymentPlanStep],
                     effects: Seq[OperationEffect],
                     outdatedBy: Option[Long])
  extends DeploymentState
