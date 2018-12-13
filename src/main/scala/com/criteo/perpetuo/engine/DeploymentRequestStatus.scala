package com.criteo.perpetuo.engine

import com.criteo.perpetuo.engine.DeploymentStatus.{failed, flopped, inProgress, succeeded}
import com.criteo.perpetuo.model._


// todo: try to make use of it in DbBinding.findDeploymentRequestsWithStatuses (because the logic is duplicated)
object computeDeploymentStatus {
  def apply(deploymentPlanStepIds: Seq[Long],
            lastOperationEffect: Option[OperationEffect]): DeploymentStatus.Value =
    lastOperationEffect
      .map { effect =>
        val lastOperation = effect.operationTrace
        val state =
          OperationEffectState.from(
            lastOperation.closingDate.isEmpty,
            effect.executionTraces.map(_.state),
            effect.targetStatuses.map(_.code)
          ) match {
            case OperationEffectState.inProgress => inProgress
            case OperationEffectState.flopped => flopped
            case OperationEffectState.failed => failed
            case OperationEffectState.succeeded => succeeded
          }
        val isIncompleteSuccessfulDeploy = lastOperation.kind == Operation.deploy && state == DeploymentStatus.succeeded && effect.deploymentPlanStepIds.max < deploymentPlanStepIds.max
        val isIncompleteRevert = lastOperation.kind == Operation.revert && deploymentPlanStepIds.min < effect.deploymentPlanStepIds.min
        if (isIncompleteSuccessfulDeploy || isIncompleteRevert)
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
