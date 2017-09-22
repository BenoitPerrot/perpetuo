package com.criteo.perpetuo.auth

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.model.Operation


class PermissionsByOperationAndUsername(config: AppConfig) extends Permissions {
  override def isAuthorized(username: String, action: GeneralAction.Value): Boolean = {
    config
      .tryGet(action.toString)
      .forall { allowed: Seq[String] => allowed.contains(username) }
  }

  override def isAuthorized(username: String, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]): Boolean = {
    val prefix = action match {
      case DeploymentAction.requestOperation => "request"
      case DeploymentAction.applyOperation => "apply"
    }
    config
      .tryGet(prefix + operation.toString.capitalize)
      .orElse(config.tryGet(action.toString))
      .forall { allowed: Seq[String] => allowed.contains(username) }
  }
}
