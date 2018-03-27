package com.criteo.perpetuo.engine

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.auth.Permissions
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.{DbBinding, UnknownProduct}
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try


object OperationStatus extends Enumeration {
  val inProgress = Value("inProgress")
  val flopped = Value("flopped")
  val failed = Value("failed")
  val succeeded = Value("succeeded")
}

abstract class RejectingError extends RuntimeException {
  val msg: String
  val detail: Map[String, _]

  def copy(newMessage: String): RejectingError

  override def getMessage: String = msg + detail.map {
    case (k, v: Iterable[_]) => s"; $k: ${v.mkString(", ")}"
    case (k, v) => s"; $k: $v"
  }.mkString("")
}

case class UnavailableAction(msg: String, detail: Map[String, _] = Map()) extends RejectingError {
  def copy(newMessage: String) = UnavailableAction(newMessage, detail)
}

case class MissingInfo(msg: String, required: String) extends RejectingError {
  val detail: Map[String, String] = Map("required" -> required)

  def copy(newMessage: String) = MissingInfo(newMessage, required)
}

case class Conflict(msg: String, conflicts: Iterable[_] = Seq()) extends RejectingError {
  val detail: Map[String, Iterable[_]] = if (conflicts.nonEmpty) Map("conflicts" -> conflicts) else Map()

  def copy(newMessage: String) = Conflict(newMessage, conflicts)
}


@Singleton
class Engine @Inject()(val dbBinding: DbBinding,
                       val targetResolver: TargetResolver,
                       val targetDispatcher: TargetDispatcher,
                       val permissions: Permissions,
                       val listeners: Seq[AsyncListener]) extends Logging {

  // todo: cosmetics in attributes
  val config = AppConfigProvider.config

  private val withTransactions = !Try(config.getBoolean("noTransactions")).getOrElse(false)

  private val operationStarter = new OperationStarter(dbBinding)

  // todo: remove once new workflow is completely in place <<
  lazy val productsExcludedFromNewWorkflow: Seq[String] = config.getOrElse("productsExcludedFromNewWorkflow", Seq())

  def isCoveredByOldWorkflow(productName: String): Boolean =
    productsExcludedFromNewWorkflow.contains(productName)

  // >>

  def getProductNames: Future[Seq[String]] =
    dbBinding.getProductNames

  def insertProduct(productName: String): Future[Product] =
    dbBinding.insertProduct(productName)

  def insertProductIfNotExists(productName: String): Future[Product] =
    dbBinding.insertProductIfNotExists(productName)

  def findDeepDeploymentRequestById(deploymentRequestId: Long): Future[Option[DeepDeploymentRequest]] =
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId)

  def createDeploymentRequest(attrs: DeploymentRequestAttrs): Future[Map[String, Any]] = {
    // todo: replace that by the creation of all the related records when the first deploy operation will be created simultaneously
    targetDispatcher.freezeParameters(attrs.productName, attrs.version)
    operationStarter.expandTarget(targetResolver, attrs.productName, attrs.version, attrs.parsedTarget)

    // todo: remove once new workflow is completely in place <<
    if (isCoveredByOldWorkflow(attrs.productName)) {
      dbBinding.findProductByName(attrs.productName)
        .map(_.map(DeepDeploymentRequest(0, _, attrs.version, attrs.target, attrs.comment, attrs.creator, attrs.creationDate))
          .getOrElse {
            throw new UnknownProduct(attrs.productName)
          })
        .flatMap(depReq =>
          Future
            .sequence(listeners.map(_.onDeploymentRequestCreated(depReq)))
            .map(x => Map("ticketUrl" -> x.filter(_ != null).mkString("")))
        )
    } // >>
    else {
      dbBinding
        .insertDeploymentRequest(attrs)
        .map { deploymentRequest =>
          Future.sequence(listeners.map(_.onDeploymentRequestCreated(deploymentRequest)))

          Map("id" -> deploymentRequest.id)
        }
    }
  }

  private def getOperationLockName(deploymentRequest: DeploymentRequest) =
    s"Operating on ${deploymentRequest.id}"

  private def getTransactionLockNames(deploymentRequest: DeepDeploymentRequest, atoms: Option[Set[String]]): Iterable[String] =
    atoms
      .map(_.map(atom => s"${atom.hashCode.toHexString}_${deploymentRequest.product.name}"))
      .getOrElse(Seq("P_" + deploymentRequest.product.name))

  private def releaseLocks(deploymentRequest: DeploymentRequest, transactionOngoing: Boolean) =
    if (transactionOngoing && withTransactions)
      dbBinding.releaseLock(getOperationLockName(deploymentRequest), deploymentRequest.id) // keep the locks per product/target
    else
      dbBinding.releaseLocks(deploymentRequest.id)

  private def startOperation(deploymentRequest: DeepDeploymentRequest,
                             operationStartSpecifics: OperationStartSpecifics): Future[(ShallowOperationTrace, Int, Int)] =
    operationStartSpecifics
      .flatMap { case (recordsCreation, atoms) =>
        dbBinding.executeInSerializableTransaction(
          dbBinding.tryAcquireLocks(Seq(getOperationLockName(deploymentRequest)), deploymentRequest.id, reentrant = false).flatMap { alreadyRunning =>
            if (alreadyRunning.nonEmpty)
              throw Conflict(
                "Cannot be processed for the moment because another operation is running for the same deployment request"
              )

            dbBinding.tryAcquireLocks(getTransactionLockNames(deploymentRequest, atoms), deploymentRequest.id, reentrant = true).flatMap { conflictingRequestIds =>
              if (conflictingRequestIds.nonEmpty)
                throw Conflict(
                  "Cannot be processed for the moment because a conflicting transaction is ongoing, which must first succeed or be reverted",
                  conflictingRequestIds
                )

              recordsCreation
            }
          }
        )
      }
      .flatMap { case (createdOperation, executionsToTrigger) =>
        operationStarter.triggerExecutions(deploymentRequest, executionsToTrigger).flatMap(effects =>
          Future.traverse(effects) { case (status, execTraceId, executionUpdate) =>
            executionUpdate
              .map { case (state, detail, logHref) =>
                updateExecutionTrace(execTraceId, state, detail, logHref) // will close the operation if there is no more ongoing execution
                  .map(_ => status)
                  .recover { case e =>
                    // only log update errors, which are not critical in that case
                    logger.error(s"Could not update execution trace #$execTraceId${logHref.map(s => s" ($s)").getOrElse("")} as $state: $detail", e)
                    status
                  }
              }
              .getOrElse(Future.successful(status))
          }.map { statuses =>
            val (successes, failures) = statuses.partition(identity)
            (createdOperation, successes.size, failures.size)
          }
        )
      }

  private def closeOperation(operationTrace: ShallowOperationTrace, deploymentRequest: DeepDeploymentRequest): Future[ShallowOperationTrace] =
    dbBinding.closeOperationTrace(operationTrace)
      .map(_.map((_, true)).getOrElse((operationTrace, false)))
      .flatMap { case (trace, updated) =>
        dbBinding.getOperationEffect(trace)
          .flatMap { effect =>
            // todo: if there still are unfinished target statuses, update them here
            val (kind, status) = computeState(effect)
            val transactionOngoing = kind == Operation.deploy && status == OperationStatus.failed
            if (updated) {
              val handler = if (status == OperationStatus.succeeded)
                (_: AsyncListener).onOperationSucceeded _
              else
                (_: AsyncListener).onOperationFailed _
              listeners.foreach(listener => handler(listener)(trace, deploymentRequest))
            }
            releaseLocks(deploymentRequest, transactionOngoing)
          }
          .map(_ => trace)
      }

  def isDeploymentRequestStarted(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Boolean)]] =
    dbBinding.isDeploymentRequestStarted(deploymentRequestId)

  private def rejectIfOutdatedOrLocked(deploymentRequest: DeploymentRequest): Future[Unit] =
    Future.sequence(Seq(
      dbBinding.lockExists(getOperationLockName(deploymentRequest)).flatMap(
        if (_)
          Future.failed(Conflict(s"${deploymentRequest.id}: an operation is still running for it"))
        else
          Future.successful()
      ),
      dbBinding.isOutdated(deploymentRequest).flatMap(
        if (_)
          Future.failed(Conflict(s"${deploymentRequest.id}: a newer one has already been applied"))
        else
          Future.successful()
      )
    )).map(_ => ())

  def canDeployDeploymentRequest(deploymentRequest: DeploymentRequest): Future[Unit] =
    rejectIfOutdatedOrLocked(deploymentRequest)

  def canRevertDeploymentRequest(deploymentRequest: DeploymentRequest, isStarted: Boolean): Future[Unit] =
    if (!isStarted)
      Future.failed(UnavailableAction("it has not yet been applied"))
    else {
      // todo: now we can allow successive rollbacks,
      // by using dbBinding.findTargetAtomNotActionableBy instead of `outdated` here
      rejectIfOutdatedOrLocked(deploymentRequest)
    }

  def startDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[ShallowOperationTrace]] = {
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId).flatMap(_
      .map(req =>
        pureStartDeploymentRequest(req, initiatorName)
          .map { case (operationTrace, started, failed) =>
            listeners.foreach(_.onDeploymentRequestStarted(req, started, failed))
            Some(operationTrace)
          }
      )
      .getOrElse(Future.successful(None))
    )
  }

  def pureStartDeploymentRequest(deploymentRequest: DeepDeploymentRequest, initiatorName: String): Future[(ShallowOperationTrace, Int, Int)] =
    startOperation(
      deploymentRequest,
      operationStarter.startDeploymentRequest(targetResolver, targetDispatcher, deploymentRequest, initiatorName)
    )

  def deployAgain(deploymentRequestId: Long, initiatorName: String): Future[Option[ShallowOperationTrace]] = {
    dbBinding.findDeepDeploymentRequestAndSpecs(deploymentRequestId).flatMap(
      _.map { case (deploymentRequest, executionSpecs) =>
        startOperation(deploymentRequest, operationStarter.deployAgain(targetResolver, targetDispatcher, deploymentRequest, executionSpecs, initiatorName))
          .map { case (operationTrace, started, failed) =>
            listeners.foreach(_.onDeploymentRequestRetried(deploymentRequest, started, failed))
            Some(operationTrace)
          }
      }.getOrElse(Future.successful(None))
    )
  }

  def revert(deploymentRequestId: Long, initiatorName: String, defaultVersion: Option[Version]): Future[Option[ShallowOperationTrace]] =
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId).flatMap(
      _.map { depReq =>
        startOperation(depReq, operationStarter.revert(targetDispatcher, depReq, initiatorName, defaultVersion))
          .map { case (operationTrace, started, failed) =>
            listeners.foreach(_.onDeploymentRequestReverted(depReq, started, failed))
            Some(operationTrace)
          }
      }.getOrElse(Future.successful(None))
    )

  def findExecutionSpecificationsForRevert(deploymentRequest: DeploymentRequest): Future[(Select, Iterable[(ExecutionSpecification, Select)])] =
    dbBinding.findExecutionSpecificationsForRevert(deploymentRequest)

  def findOperationTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowOperationTrace]]] =
    dbBinding.findOperationTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowExecutionTrace]]] =
    dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def updateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, logHref: Option[String], statusMap: Map[String, TargetAtomStatus] = Map()): Future[Option[Unit]] = {
    dbBinding.updateExecutionTrace(id, executionState, detail, logHref).flatMap(_
      .map(_ => // the execution trace exists
        dbBinding.findExecutionTraceById(id).map(_.get).flatMap { execTrace =>
          val op = execTrace.operationTrace

          dbBinding.dbContext.db.run(dbBinding.updateTargetStatuses(execTrace.executionId, statusMap))
            .flatMap(_ => dbBinding.hasOpenExecutionTracesForOperation(op.id))
            .flatMap { hasOpenExecutions =>
              if (hasOpenExecutions)
                Future.successful(Some())
              else
                dbBinding.findDeepDeploymentRequestById(op.deploymentRequestId).flatMap { depReq =>
                  closeOperation(op, depReq.get).map(_ => Some())
                }
            }
        }
      )
      .getOrElse(Future.successful(None))
    )
  }

  def findDeepDeploymentRequestAndEffects(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Iterable[OperationEffect])]] =
    dbBinding.findDeepDeploymentRequestAndEffects(deploymentRequestId)

  def queryDeepDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[(DeepDeploymentRequest, Option[OperationEffect])]] =
    dbBinding.deepQueryDeploymentRequests(where, orderBy, limit, offset)

  def computeState(operationEffect: OperationEffect): (Operation.Kind, OperationStatus.Value) = {
    val OperationEffect(operationTrace, executionTraces, targetStatuses) = operationEffect
    val lastOperationState = operationTrace.closingDate.map { _ =>
      if (targetStatuses.forall(_.code == Status.notDone))
        OperationStatus.flopped
      else if (targetStatuses.forall(_.code == Status.success) && executionTraces.forall(_.state == ExecutionState.completed))
        OperationStatus.succeeded
      else
        OperationStatus.failed
    }.getOrElse(
      OperationStatus.inProgress
    )
    (operationTrace.kind, lastOperationState)
  }

  /**
    * Compute the version that should be used in order to align the given target
    * with the main version deployed lately in reference targets, if applicable.
    *
    * @param referenceAtoms : ordered sequence of sets of target *atoms* whose "current" versions
    *                       must be inferred from the deployment history; the queries are applied
    *                       per set and in sequence until one responds at least a version.
    * @return the majority version of the first reference pool for which a version is found
    */
  def computeDominantVersion(productName: String, referenceAtoms: Iterable[Iterable[String]]): Future[Option[Version]] = {
    // the fold function to chain the asynchronous queries as long as we get no result
    def chainQueries(acc: Future[Map[String, Version]], atomsGroup: Iterable[String]) =
      acc.flatMap(lastVersions =>
        if (lastVersions.isEmpty) // run the query below until we get a non-empty result
          dbBinding.findCurrentVersionForEachKnownTarget(productName, atomsGroup)
        else
          Future.successful(lastVersions)
      )

    referenceAtoms
      .foldLeft(Future.successful(Map[String, Version]()))(chainQueries)
      .map { knownVersions =>
        // get the most represented version in that pool
        knownVersions.headOption.map { _ =>
          val (version, _) = knownVersions.values.groupBy(identity).maxBy { case (_, list) => list.size }
          version
        }
      }
  }

}
