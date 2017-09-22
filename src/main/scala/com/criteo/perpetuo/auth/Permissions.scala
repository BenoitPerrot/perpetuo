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
  def isAuthorized(username: String, action: GeneralAction.Value): Boolean

  def isAuthorized(username: String, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]): Boolean
}


class Unrestricted extends Permissions {
  override def isAuthorized(username: String, action: GeneralAction.Value) = true

  override def isAuthorized(username: String, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]) = true
}
