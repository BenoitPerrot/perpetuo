package com.criteo.perpetuo.dao

import com.twitter.inject.Logging
import slick.jdbc.TransactionIsolation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class LockRecord(name: String,
                                   deploymentRequestId: Long)


trait LockBinder extends TableBinder with Logging {
  this: DeploymentRequestBinder with DbContextProvider =>

  import dbContext.driver.api._

  class LockTable(tag: Tag) extends Table[LockRecord](tag, "lock") {
    def name = column[String]("name", O.SqlType("nvarchar(128)"))
    protected def pk = primaryKey(name)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def * = (name, deploymentRequestId) <> (LockRecord.tupled, LockRecord.unapply)
  }

  val lockQuery: TableQuery[LockTable] = TableQuery[LockTable]

  type DeploymentRequestId = Long

  /**
    * Acquire all the locks on behalf of the given deployment request ID if possible,
    * otherwise don't change anything but return the IDs of the deployment requests owning the conflicting locks.
    *
    * @param reentrant either locks are considered acquired (if true) or conflicting (if false) when they are
    *                  already owned by the requester
    */
  def tryAcquireLocks(names: Iterable[String], acquiringDeploymentRequestId: DeploymentRequestId, reentrant: Boolean): Future[Iterable[DeploymentRequestId]] = {
    val q = lockQuery.filter(_.name.inSet(names)).result.flatMap { previouslyAcquired =>
      val (namesToAcquire, conflicting) = if (reentrant) {
        val (toIgnore, conflicting) = previouslyAcquired.toStream.partition(_.deploymentRequestId == acquiringDeploymentRequestId)
        val ignoredNames = toIgnore.map(_.name).toSet
        (names.toStream.filter(!ignoredNames.contains(_)), conflicting)
      }
      else
        (names, previouslyAcquired)

      val conflictingIds = conflicting.map(lock => lock.deploymentRequestId -> lock.name).toMap
      if (conflictingIds.isEmpty)
        (lockQuery ++= namesToAcquire.map(LockRecord(_, acquiringDeploymentRequestId))).map(_ => conflictingIds /* empty */)
      else
        DBIO.successful(conflictingIds)
    }

    dbContext.db.run(q.transactionally.withTransactionIsolation(TransactionIsolation.Serializable)).map { conflictSamples =>
      conflictSamples.map { case (ownerId, name) =>
        logger.debug(s"Couldn't acquire lock `$name` for request #$acquiringDeploymentRequestId because request #$ownerId owns it")
        ownerId
      }
    }
  }

  /**
    * Release all the locks (in the same transaction) owned by the given deployment request ID.
    *
    * @return the number of locks that have actually been released.
    */
  def releaseLocks(owningDeploymentRequestId: DeploymentRequestId): Future[Int] =
    dbContext.db.run(lockQuery.filter(_.deploymentRequestId === owningDeploymentRequestId).delete.transactionally)

  /**
    * Release the given lock if acquired by the given deployment request, otherwise do nothing.
    *
    * @return the number of locks that have actually been released (so 1 or 0).
    */
  def releaseLock(name: String, owningDeploymentRequestId: DeploymentRequestId): Future[Int] =
    dbContext.db.run(lockQuery.filter(lock => lock.name === name && lock.deploymentRequestId === owningDeploymentRequestId).delete)

  def lockExists(name: String): Future[Boolean] =
    dbContext.db.run(lockQuery.filter(_.name === name).exists.result)

}
