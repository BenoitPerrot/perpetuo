package com.criteo.perpetuo.auth

import com.criteo.perpetuo.model.Operation


object GeneralAction extends Enumeration {
  val administrate = Value("administrate")
  val addProduct = Value("addProduct")
}


object DeploymentAction extends Enumeration {
  val requestOperation = Value("requestOperation")
  val applyOperation = Value("applyOperation")
}


trait Permissions {
  def isAuthorized(user: User, action: GeneralAction.Value): Boolean

  def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]): Boolean
}


object Unrestricted extends Permissions {
  override def isAuthorized(user: User, action: GeneralAction.Value) = true

  override def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, target: Iterable[String]) = true
}
