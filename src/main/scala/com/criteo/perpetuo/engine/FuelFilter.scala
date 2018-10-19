package com.criteo.perpetuo.engine

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.{DBIOrw, DbBinding, LockName}
import com.criteo.perpetuo.model.{DeploymentRequest, TargetAtom}
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext.Implicits.global

case class OperationLockAlreadyTaken() extends RuntimeException

class FuelFilter(dbBinding: DbBinding) {
  private val withTransactions = !AppConfigProvider.config.tryGetBoolean("noTransactions").getOrElse(false)

  def acquiringOperationLock(deploymentRequest: DeploymentRequest): DBIOrw[Unit] =
    dbBinding.tryAcquireLocks(Seq(getOperationLockName(deploymentRequest)), deploymentRequest.id, reentrant = false).map(alreadyRunning =>
      if (alreadyRunning.nonEmpty)
        throw OperationLockAlreadyTaken()
    )

  def acquiringDeploymentTransactionLock(deploymentRequest: DeploymentRequest, atoms: Set[TargetAtom]): DBIOrw[Unit] =
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

  private def getOperationLockName(deploymentRequest: DeploymentRequest) =
    s"Operating on ${deploymentRequest.id}"

  private def getTransactionLockNames(deploymentRequest: DeploymentRequest, atoms: Set[TargetAtom]): Iterable[String] =
    atoms.map { atom =>
      val atomId = atom.hashCode.toHexString
      // the hexadecimal representation of a hash code (int => 32 bits) is 8-character long maximum (2^32 = 16^8)
      if (LockName.maxLength <= 8 + deploymentRequest.product.name.length)
      // the product name is too long to *always* fit, take its ID instead (10-char max.)
        s"$atomId#${deploymentRequest.product.id}"
      else
      // whenever we can, keep the product name to make the locks readable in DB
        s"${atomId}_${deploymentRequest.product.name}"
    }
}
