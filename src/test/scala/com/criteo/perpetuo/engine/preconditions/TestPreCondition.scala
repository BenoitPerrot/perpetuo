package com.criteo.perpetuo.engine.preconditions

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.config.DefaultPreConditionPlugin
import com.criteo.perpetuo.model.TargetAtom

import scala.util.{Failure, Success, Try}

class TestPreCondition extends DefaultPreConditionPlugin  {
  override def canRequestDeployment(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit] =
    if (productName == "nonRequestableProduct")
      Failure(new IllegalArgumentException(""))
    else
      Success(())
}
