package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model._


// todo: try to make use of it in DbBinding.findDeploymentRequestsWithStatuses (because the logic is duplicated)
object computeDeploymentStatus {
  def apply(deploymentPlanStepIds: Seq[Long],
            lastOperationEffect: Option[OperationEffect]): DeploymentStatus.Value =
    lastOperationEffect
      .map { effect =>
        val lastOperation = effect.operationTrace
        val state = computeOperationState(
          lastOperation.closingDate.isEmpty,
          effect.executionTraces.map(_.state),
          effect.targetStatuses.map(_.code)
        )
        val forward = lastOperation.kind == Operation.deploy
        val notFinished = if (forward)
          effect.deploymentPlanStepIds.max < deploymentPlanStepIds.max
        else
          deploymentPlanStepIds.min < effect.deploymentPlanStepIds.min
        if (notFinished && (state == DeploymentStatus.succeeded || !forward))
          DeploymentStatus.paused
        else
          state
      }
      .getOrElse(DeploymentStatus.notStarted)
}

trait DeploymentState {
  val deploymentRequest: DeploymentRequest
  val deploymentPlanSteps: Seq[DeploymentPlanStep]
  val effects: Seq[OperationEffect]
  val outdatedBy: Option[Long]

  def isOutdated: Boolean = outdatedBy.nonEmpty
}

trait InProgressState extends DeploymentState {
  val scope: OperationEffect
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
                        scope: Seq[DeploymentPlanStep],
                        outdatedBy: Option[Long])
  extends DeploymentState

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
  extends DeploymentState

case class Paused(deploymentRequest: DeploymentRequest,
                  deploymentPlanSteps: Seq[DeploymentPlanStep],
                  effects: Seq[OperationEffect],
                  toDo: DeploymentPlanStep,
                  lastDone: DeploymentPlanStep,
                  revertScope: Seq[DeploymentPlanStep],
                  outdatedBy: Option[Long])
  extends DeploymentState

case class Deployed(deploymentRequest: DeploymentRequest,
                    deploymentPlanSteps: Seq[DeploymentPlanStep],
                    effects: Seq[OperationEffect],
                    revertScope: Seq[DeploymentPlanStep],
                    outdatedBy: Option[Long])
  extends DeploymentState

case class Abandoned(deploymentRequest: DeploymentRequest,
                     deploymentPlanSteps: Seq[DeploymentPlanStep],
                     effects: Seq[OperationEffect],
                     outdatedBy: Option[Long])
  extends DeploymentState
