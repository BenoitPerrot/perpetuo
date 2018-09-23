package com.criteo.perpetuo.engine

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.{DBIOrw, DbBinding}
import com.criteo.perpetuo.model.{DeploymentPlanStep, DeploymentRequest, TargetAtomSet}
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext.Implicits.global


class FuelFilter(dbBinding: DbBinding) {
  private val withTransactions = !AppConfigProvider.config.tryGetBoolean("noTransactions").getOrElse(false)

  def acquiringOperationLock(deploymentRequest: DeploymentRequest): DBIOrw[Unit] =
    dbBinding.tryAcquireLocks(Seq(getOperationLockName(deploymentRequest)), deploymentRequest.id, reentrant = false).map(alreadyRunning =>
      if (alreadyRunning.nonEmpty)
        throw Conflict("Cannot be processed for the moment because another operation is running for the same deployment request", deploymentRequest.id)
    )

  def acquiringDeploymentTransactionLock(deploymentRequest: DeploymentRequest, atoms: Option[TargetAtomSet]): DBIOrw[Unit] =
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

  //
  // PRIVATE METHODS:

  // TODO: inline in (at least move closer to) lone caller in Crankshaft
  def findNextPlanStep(planSteps: Seq[DeploymentPlanStep], referencePlanStepId: Long): Option[DeploymentPlanStep] =
    planSteps.foldLeft(None: Option[DeploymentPlanStep]) { (result, x) =>
      if (referencePlanStepId < x.id && result.forall(x.id < _.id))
        Some(x)
      else
        result
    }

  private def getOperationLockName(deploymentRequest: DeploymentRequest) =
    s"Operating on ${deploymentRequest.id}"

  private def getTransactionLockNames(deploymentRequest: DeploymentRequest, atoms: Option[TargetAtomSet]): Iterable[String] =
    atoms
      .map(_.items.map(atom => s"${atom.hashCode.toHexString}_${deploymentRequest.product.name}"))
      .getOrElse(Seq("P_" + deploymentRequest.product.name))
}
