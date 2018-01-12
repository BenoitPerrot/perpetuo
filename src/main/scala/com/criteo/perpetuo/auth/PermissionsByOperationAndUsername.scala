package com.criteo.perpetuo.auth

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.model.Operation
import com.typesafe.config.Config


class PermissionsByOperationAndUsername(config: Config) extends Permissions {
  override def isAuthorized(user: User, action: GeneralAction.Value): Boolean = {
    config
      .tryGet(action.toString)
      .forall { allowed: Seq[String] => allowed.contains(user.name) }
  }

  override def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]): Boolean = {
    val prefix = action match {
      case DeploymentAction.requestOperation => "request"
      case DeploymentAction.applyOperation => "apply"
    }
    config
      .tryGet(prefix + operation.toString.capitalize)
      .orElse(config.tryGet(action.toString))
      .forall { allowed: Seq[String] => allowed.contains(user.name) }
  }
}
