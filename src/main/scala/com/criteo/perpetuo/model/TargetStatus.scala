package com.criteo.perpetuo.model


case class TargetAtomStatus(code: Status.Code, detail: String)

case class TargetStatus(targetAtom: String,
                        code: Status.Code,
                        detail: String)

object TargetAtom {
  type Type = String
  val maxSize = 128
}

object Status extends Enumeration {
  type Code = Value

  // these values must be stable, since they are persisted in the DB
  val running = Value(0, "running")
  val success = Value(1, "success")
  val productFailure = Value(2, "productFailure")
  val hostFailure = Value(3, "hostFailure")
  val notDone = Value(4, "notDone")
}
