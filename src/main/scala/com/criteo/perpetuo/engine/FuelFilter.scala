package com.criteo.perpetuo.engine

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.{DBIOrw, DbBinding}
import com.criteo.perpetuo.model.{DeploymentPlanStep, DeploymentRequest, Operation}
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext.Implicits.global


class FuelFilter(dbBinding: DbBinding) {
  private val withTransactions = !AppConfigProvider.config.tryGetBoolean("noTransactions").getOrElse(false)

  def acquiringOperationLock(deploymentRequest: DeploymentRequest): DBIOrw[Unit] =
    dbBinding.tryAcquireLocks(Seq(getOperationLockName(deploymentRequest)), deploymentRequest.id, reentrant = false).map(alreadyRunning =>
      if (alreadyRunning.nonEmpty)
        throw Conflict("Cannot be processed for the moment because another operation is running for the same deployment request", deploymentRequest.id)
    )

  def acquiringDeploymentTransactionLock(deploymentRequest: DeploymentRequest, atoms: Option[Select]): DBIOrw[Unit] =
    dbBinding.tryAcquireLocks(getTransactionLockNames(deploymentRequest, atoms), deploymentRequest.id, reentrant = true).map(conflictingRequestIds =>
      if (conflictingRequestIds.nonEmpty)
        throw Conflict("Cannot be processed for the moment because a conflicting transaction is ongoing, which must first succeed or be reverted", deploymentRequest.id, conflictingRequestIds)
    )

  def releasingLocks(deploymentRequest: DeploymentRequest, transactionOngoing: Boolean): DBIOAction[Int, NoStream, Effect.Write with Effect.Transactional] =
    if (transactionOngoing && withTransactions)
      dbBinding.releasingLock(getOperationLockName(deploymentRequest), deploymentRequest.id) // keep the locks per product/target
    else
      dbBinding.releasingLocks(deploymentRequest.id)

  def rejectingIfLocked(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read] =
    dbBinding.lockExists(getOperationLockName(deploymentRequest)).flatMap(
      if (_)
        DBIOAction.failed(Conflict("an operation is still running for it", deploymentRequest.id))
      else
        DBIOAction.successful(())
    )

  def gettingPlanStepToOperateAndLastDoneStep(deploymentRequest: DeploymentRequest, operationToDo: Operation.Kind): DBIOAction[(DeploymentPlanStep, Option[DeploymentPlanStep]), NoStream, Effect.Read] =
    rejectingIfOutdated(deploymentRequest)
      .andThen(dbBinding.findingDeploymentPlanAndLatestOperations(deploymentRequest))
      .flatMap(planStepsAndLatestOperations =>
        if (planStepsAndLatestOperations.isEmpty)
          DBIOAction.failed(new RuntimeException(s"${deploymentRequest.id}: should not be there: deployment plan is empty"))
        else
          gettingPlanStepToOperateAndLastDoneStep(planStepsAndLatestOperations, operationToDo)
      )

  def rejectingIfCannotDeploy(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read] =
    gettingPlanStepToOperateAndLastDoneStep(deploymentRequest, Operation.deploy).map(_ => ())

  // todo: now we can allow successive rollbacks, by using dbBinding.findTargetAtomNotActionableBy instead of `outdated` here
  def rejectingIfCannotRevert(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read] =
    gettingPlanStepToOperateAndLastDoneStep(deploymentRequest, Operation.revert)
      .andThen(rejectingIfNothingToRevert(deploymentRequest))

  //
  // PRIVATE METHODS:

  private def getLastDoneStepAndOperation(planStepsAndLatestOperations: Seq[(DeploymentPlanStep, Option[(Long, Operation.Kind)])]) =
    planStepsAndLatestOperations.reduce[(DeploymentPlanStep, Option[(Long, Operation.Kind)])] {
      case ((latestPlanStep, None), current@(planStep, None)) if planStep.id < latestPlanStep.id =>
        // When comparing two steps, both with no operation, the oldest one is the latest one
        current

      case ((_, Some((latestOperationId, _))), current@(_, Some((operationId, _)))) if latestOperationId < operationId =>
        // When comparing two steps, both with operations, the step with the youngest operation is the latest one
        current

      case ((_, None), current@(_, Some(_))) =>
        // When comparing a step with no operation to another with operations, the one with operations is the latest one
        current

      case (latest, _) =>
        // When all conditions failed, the latest one is to be kept
        latest
    }

  private def findNextPlanStep(planSteps: Seq[DeploymentPlanStep], referencePlanStep: DeploymentPlanStep) =
    planSteps.foldLeft(None: Option[DeploymentPlanStep]) { (result, x) =>
      if (referencePlanStep.id < x.id && result.forall(x.id < _.id))
        Some(x)
      else
        result
    }

  private def gettingPlanStepToOperateAndLastDoneStep(planStepsAndLatestOperations: Seq[(DeploymentPlanStep, Option[(Long, Operation.Kind)])], operationToDo: Operation.Kind) =
    getLastDoneStepAndOperation(planStepsAndLatestOperations) match {
      case (latestPlanStep, None) =>
        DBIOAction.successful((latestPlanStep, None))

      case (latestPlanStep, Some((lastOperationId, lastOperationKind))) =>
        dbBinding.computingOperationStatus(lastOperationId, isRunning = false)
          .map(operationStatus =>
            if (lastOperationKind == Operation.revert && operationToDo == Operation.deploy)
              throw UnprocessableIntent(s"${latestPlanStep.deploymentRequest.id}: deploying after a revert is not supported")
            else if (lastOperationKind != operationToDo || operationStatus == DeploymentStatus.flopped || operationStatus == DeploymentStatus.failed)
              latestPlanStep
            else if (operationToDo == Operation.revert) // ONLY AS LONG AS REVERT OPERATIONS ARE ALWAYS FULL REVERTS (fixme: consider all the steps related to the last operation)
              throw UnprocessableIntent(s"${latestPlanStep.deploymentRequest.id}: there is no next step, they have all been successfully reverted")
            else
              findNextPlanStep(planStepsAndLatestOperations.map(_._1), latestPlanStep)
                .getOrElse(throw UnprocessableIntent(s"${latestPlanStep.deploymentRequest.id}: there is no next step, they have all been successfully deployed"))
          )
          .map((_, Some(latestPlanStep)))
    }

  private def rejectingIfNothingToRevert(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read] =
    dbBinding.hasHadAnEffect(deploymentRequest.id).flatMap(
      if (_)
        DBIOAction.successful(())
      else
        DBIOAction.failed(UnavailableAction(s"${deploymentRequest.id}: Nothing to revert"))
    )

  // fixme: race condition
  private def rejectingIfOutdated(deploymentRequest: DeploymentRequest) =
    dbBinding.isOutdated(deploymentRequest).flatMap(
      if (_)
        DBIOAction.failed(Conflict("a newer one has already been applied", deploymentRequest.id))
      else
        DBIOAction.successful(())
    )

  private def getOperationLockName(deploymentRequest: DeploymentRequest) =
    s"Operating on ${deploymentRequest.id}"

  private def getTransactionLockNames(deploymentRequest: DeploymentRequest, atoms: Option[Select]): Iterable[String] =
    atoms
      .map(_.map(atom => s"${atom.hashCode.toHexString}_${deploymentRequest.product.name}"))
      .getOrElse(Seq("P_" + deploymentRequest.product.name))
}
