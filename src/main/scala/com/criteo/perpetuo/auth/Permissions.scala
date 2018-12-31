package com.criteo.perpetuo.auth

import com.criteo.perpetuo.model.{Operation, TargetAtom}


object GeneralAction extends Enumeration {
  val administrate = Value("administrate")
  val updateProduct = Value("updateProduct")
}


object DeploymentAction extends Enumeration {
  val requestOperation = Value("requestOperation")
  val applyOperation = Value("applyOperation")
  val stopOperation = Value("stopOperation")
  val abandonOperation = Value("abandonOperation")
}


trait Permissions {
  def isAuthorized(user: User, action: GeneralAction.Value): Boolean

  /**
    * Is the user authorized to request or apply the given operation for the given product
    * on the given targets? Note that the right to stop a deployment is the same as the
    * right to revert a deployment.
    */
  def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, targets: Set[TargetAtom]): Boolean
}


object Unrestricted extends Permissions {
  override def isAuthorized(user: User, action: GeneralAction.Value) = true

  override def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String, targets: Set[TargetAtom]): Boolean = true
}
