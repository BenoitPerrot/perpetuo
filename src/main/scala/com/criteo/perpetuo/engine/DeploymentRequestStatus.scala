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


case class DeployScope(toDo: DeploymentPlanStep, lastDone: Option[DeploymentPlanStep])

case class ApplicableActions(deployScope: Option[DeployScope] = None,
                             revertScope: Option[Seq[DeploymentPlanStep]] = None,
                             stopScope: Option[OperationEffect] = None)

case class DeploymentState(deploymentRequest: DeploymentRequest,
                           deploymentPlanSteps: Seq[DeploymentPlanStep],
                           isOutdated: Boolean,
                           effects: Seq[OperationEffect],
                           applicableActions: ApplicableActions)

// TODO: replace with DeploymentState
case class DeploymentRequestStatus(deploymentRequest: DeploymentRequest,
                                   deploymentPlanSteps: Seq[DeploymentPlanStep],
                                   operationEffects: Iterable[OperationEffect],
                                   lastOperationStatus: Option[(Operation.Kind, OperationEffectState.Value)], // fixme
                                   eligibleActions: Seq[(String, Option[String])])
