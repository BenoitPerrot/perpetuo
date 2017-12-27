package com.criteo.perpetuo.model


case class TargetAtomStatus(code: Status.Code, detail: String)

case class TargetStatus(executionId: Long,
                        targetAtom: String,
                        code: Status.Code,
                        detail: String)

object TargetAtom {
  type Type = String
  val maxSize = 128
}

object Status extends Enumeration {
  type Code = Value

  // these values must be stable, since they are persisted in the DB
  val running = Value(0)
  val success = Value(1)
  val productFailure = Value(2)
  val hostFailure = Value(3)
  val notDone = Value(4)
}
