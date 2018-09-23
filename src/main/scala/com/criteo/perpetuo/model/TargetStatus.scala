package com.criteo.perpetuo.model


case class TargetAtom(name: String) extends AnyVal

case class TargetAtomSet(items: Set[TargetAtom])

case class TargetAtomStatus(code: Status.Code, detail: String)

case class TargetStatus(targetAtom: TargetAtom,
                        code: Status.Code,
                        detail: String)

object Status extends Enumeration {
  type Code = Value

  // - the numeric values must be stable, since they are persisted in the DB
  // - the strings are used in the API, so clients must be migrated in case of change
  val running = Value(0, "running")
  val success = Value(1, "success")
  val productFailure = Value(2, "productFailure")
  val hostFailure = Value(3, "hostFailure")
  val notDone = Value(4, "notDone")
  val undetermined = Value(5, "undetermined")
}
