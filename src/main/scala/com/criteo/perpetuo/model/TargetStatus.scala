package com.criteo.perpetuo.model

object TargetStatus extends Enumeration {
  type MapType = Map[String, Value]

  // these values should be stable in short term, since they are persisted in the DB for a few hours max
  val success = Value(1)
  val deploymentFailure = Value(2)
  val serverFailure = Value(3)
  val notDone = Value(4)
}
