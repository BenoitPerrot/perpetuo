package com.criteo.perpetuo.engine

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.{DBIOrw, DbBinding}
import com.criteo.perpetuo.model.{DeploymentPlanStep, DeploymentRequest, Operation}
import com.google.common.annotations.VisibleForTesting
import javax.inject.{Inject, Singleton}
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext.Implicits.global


@Singleton
class FuelFilter @Inject()(val dbBinding: DbBinding) {
  private val withTransactions = !AppConfigProvider.config.tryGetBoolean("noTransactions").getOrElse(false)

  def acquiringOperationLock(deploymentRequest: DeploymentRequest): DBIOrw[Unit] =
    dbBinding.tryAcquireLocks(Seq(getOperationLockName(deploymentRequest)), deploymentRequest.id, reentrant = false).map(alreadyRunning =>
      if (alreadyRunning.nonEmpty)
        throw Conflict("Cannot be processed for the moment because another operation is running for the same deployment request")
    )

  def acquiringDeploymentTransactionLock(deploymentRequest: DeploymentRequest, atoms: Option[Select]): DBIOrw[Unit] =
    dbBinding.tryAcquireLocks(getTransactionLockNames(deploymentRequest, atoms), deploymentRequest.id, reentrant = true).map(conflictingRequestIds =>
      if (conflictingRequestIds.nonEmpty)
        throw Conflict("Cannot be processed for the moment because a conflicting transaction is ongoing, which must first succeed or be reverted", conflictingRequestIds)
    )

  def releasingLocks(deploymentRequest: DeploymentRequest, transactionOngoing: Boolean): DBIOAction[Int, NoStream, Effect.Write with Effect.Transactional] =
    if (transactionOngoing && withTransactions)
      dbBinding.releasingLock(getOperationLockName(deploymentRequest), deploymentRequest.id) // keep the locks per product/target
    else
      dbBinding.releasingLocks(deploymentRequest.id)

  def rejectingIfLocked(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read] =
    dbBinding.lockExists(getOperationLockName(deploymentRequest)).flatMap(
      if (_)
        DBIOAction.failed(Conflict(s"${deploymentRequest.id}: an operation is still running for it"))
      else
        DBIOAction.successful(())
    )

  def gettingPlanStepToOperateAndLastDoneStepOrRejectingIfCannotDeploy(deploymentRequest: DeploymentRequest): DBIOAction[(DeploymentPlanStep, Option[DeploymentPlanStep]), NoStream, Effect.Read] =
    rejectingIfOutdated(deploymentRequest)
      .andThen(gettingPlanStepToOperateAndLastDoneStep(deploymentRequest, Operation.deploy))
      .map(_.getOrElse(throw UnprocessableIntent(s"${deploymentRequest.id}: there is no next step, they have all been applied")))

  def rejectingIfCannotDeploy(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read] =
    gettingPlanStepToOperateAndLastDoneStepOrRejectingIfCannotDeploy(deploymentRequest).map(_ => ())

  def rejectingIfCannotRevert(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read] =
    rejectingIfOutdated(deploymentRequest) // todo: now we can allow successive rollbacks, by using dbBinding.findTargetAtomNotActionableBy instead of `outdated` here
      .andThen(rejectingIfNothingToRevert(deploymentRequest))

  //
  // PRIVATE METHODS:

  @VisibleForTesting
  def gettingPlanStepToOperateAndLastDoneStep(deploymentRequest: DeploymentRequest, operation: Operation.Kind): DBIOAction[Option[(DeploymentPlanStep, Option[DeploymentPlanStep])], NoStream, Effect.Read] =
    dbBinding.findingDeploymentPlanAndLatestOperations(deploymentRequest)
      .flatMap(planStepsAndLatestOperations =>
        if (planStepsAndLatestOperations.isEmpty)
          DBIOAction.failed(new RuntimeException(s"${deploymentRequest.id}: should not be there: deployment plan is empty"))
        else
          gettingPlanStepToOperateAndLastDoneStep(planStepsAndLatestOperations, operation)
      )

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
        DBIOAction.successful(Some((latestPlanStep, None)))

      case (latestPlanStep, Some((lastOperationId, lastOperationKind))) =>
        assert(lastOperationKind == operationToDo)
        dbBinding.computingOperationStatus(lastOperationId, isRunning = false)
          .map(operationStatus =>
            if (operationStatus == DeploymentStatus.flopped || operationStatus == DeploymentStatus.failed)
              Some(latestPlanStep)
            else
              findNextPlanStep(planStepsAndLatestOperations.map(_._1), latestPlanStep)
          )
          .map(_.map((_, Some(latestPlanStep))))
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
        DBIOAction.failed(Conflict(s"${deploymentRequest.id}: a newer one has already been applied"))
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
