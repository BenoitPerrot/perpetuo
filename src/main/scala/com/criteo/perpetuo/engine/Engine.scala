package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.{DeploymentAction, Permissions, User}
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Operation}
import javax.inject.{Inject, Singleton}

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
}
