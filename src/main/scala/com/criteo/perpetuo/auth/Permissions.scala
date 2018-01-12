package com.criteo.perpetuo.auth

import com.criteo.perpetuo.model.Operation


object GeneralAction extends Enumeration {
  val administrate = Value
  val addProduct = Value
}


object DeploymentAction extends Enumeration {
  val requestOperation = Value
  val applyOperation = Value
}


trait Permissions {
  def isAuthorized(user: User, action: GeneralAction.Value): Boolean

  def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]): Boolean
}


class Unrestricted extends Permissions {
  override def isAuthorized(user: User, action: GeneralAction.Value) = true

  override def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]) = true
}
