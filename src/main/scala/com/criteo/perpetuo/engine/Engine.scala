package com.criteo.perpetuo.engine

import com.criteo.perpetuo.app.DeploymentRequestStatus
import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, Permissions, User}
import com.criteo.perpetuo.model._
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class PermissionDenied() extends RuntimeException

@Singleton
class Engine @Inject()(val crankshaft: Crankshaft,
                       val permissions: Permissions) {
  def requestDeployment(user: User, attrs: DeploymentRequestAttrs, targets: Set[String]): Future[Long] = {
    if (!permissions.isAuthorized(user, DeploymentAction.requestOperation, Operation.deploy, attrs.productName, targets))
      throw PermissionDenied()

    crankshaft.createDeploymentRequest(attrs)
  }

  def deploy(user: User, id: Long): Future[Option[DeepOperationTrace]] =
    crankshaft.isDeploymentRequestStarted(id)
      .flatMap(
        _.map { case (deploymentRequest, isStarted) =>
          if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
            throw PermissionDenied()

          crankshaft
            .canDeployDeploymentRequest(deploymentRequest)
            .flatMap(_ =>
              if (isStarted)
                crankshaft.deployAgain(id, user.name)
              else
                crankshaft.startDeploymentRequest(id, user.name)
            )
        }.getOrElse(Future.successful(None))
      )

  def deviseRevertPlan(id: Long): Future[Option[(Select, Iterable[(ExecutionSpecification, Select)])]] =
    crankshaft.isDeploymentRequestStarted(id)
      .flatMap(
        _.map { case (deploymentRequest, isStarted) =>
          crankshaft
            .canRevertDeploymentRequest(deploymentRequest, isStarted)
            .recover { case e: RejectingError => throw e.copy(s"Cannot revert the request #${deploymentRequest.id}: ${e.msg}") } // TODO: remove the copy
            .flatMap { _ =>
              crankshaft
                .findExecutionSpecificationsForRevert(deploymentRequest)
                .map(Some.apply)
            }
        }.getOrElse(Future.successful(None))
    )

  def revert(user: User, id: Long, defaultVersion: Option[Version]): Future[Option[DeepOperationTrace]] =
    crankshaft.isDeploymentRequestStarted(id)
      .flatMap(
        _.map { case (deploymentRequest, isStarted) =>
          if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
            throw PermissionDenied()

          crankshaft
            .canRevertDeploymentRequest(deploymentRequest, isStarted)
            .flatMap(_ => crankshaft.revert(id, user.name, defaultVersion))
        }.getOrElse(Future.successful(None))
      )

  def stop(user: User, id: Long): Future[Option[(Int, Seq[String])]] =
    crankshaft.findDeepDeploymentRequestById(id)
      .flatMap(_
        .map { deploymentRequest =>
          if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
            throw PermissionDenied()

          crankshaft
            .tryStopDeploymentRequest(deploymentRequest, user.name)
            .map(Some.apply)
        }
        .getOrElse(Future.successful(None))
      )

  def queryDeploymentRequestStatus(user: Option[User], id: Long): Future[Option[DeploymentRequestStatus]] =
    crankshaft
      .findDeepDeploymentRequestAndEffects(id)
      .flatMap(
        _.map { case (deploymentRequest, sortedEffects) =>
          val targets = deploymentRequest.parsedTarget.select

          val isAdmin = user.exists(user =>
            permissions.isAuthorized(user, GeneralAction.administrate)
          )

          // todo: a future workflow will differentiate requests and applies
          def authorized(op: Operation.Kind) = user.exists(user =>
            permissions.isAuthorized(user, DeploymentAction.applyOperation, op, deploymentRequest.product.name, targets)
          )

          Future
            .sequence(
              Operation.values.toSeq.map(action =>
                (action match {
                  case Operation.deploy => crankshaft.canDeployDeploymentRequest(deploymentRequest)
                  case Operation.revert => crankshaft.canRevertDeploymentRequest(deploymentRequest, sortedEffects.nonEmpty)
                })
                  .map(_ => Some((action, authorized(action))))
                  .recover { case _ => None }
              )
            )
            .map(authorizedActions =>
              Some(DeploymentRequestStatus(deploymentRequest, sortedEffects, authorizedActions.flatten, isAdmin || authorized(Operation.deploy)))
            )

        }.getOrElse(Future.successful(None))
      )

  def insertProductIfNotExists(user: User, name: String): Future[Product] = {
    if (!permissions.isAuthorized(user, GeneralAction.addProduct))
      throw PermissionDenied()

    crankshaft.insertProductIfNotExists(name)
  }
}
