package com.criteo.perpetuo.model

object ExecutionState extends Enumeration {
  type ExecutionState = Value

  // these values must be stable, since they are persisted in the DB
  val pending = Value(1, "pending")
  val running = Value(2, "running")
  val initFailed = Value(3, "initFailed") // todo: obsolete: migrate clients and remove
  val conflicting = Value(4, "conflicting") // no need to try with another executor: abort everything and revert
  val aborted = Value(5, "aborted") // the job run has been killed on purpose but sent a [partial] result
  val lost = Value(6, "lost") // the job run is terminated but the executor never reached Perpetuo
  val completed = Value(7, "completed") // the final status has been received, either succeeded or failed (per target)
}
