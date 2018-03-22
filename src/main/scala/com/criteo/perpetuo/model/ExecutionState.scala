package com.criteo.perpetuo.model

object ExecutionState extends Enumeration {
  type ExecutionState = Value

  // these values must be stable, since they are persisted in the DB
  val pending = Value(1, "pending")
  val running = Value(2, "running")
  val initFailed = Value(3, "initFailed") // todo: obsolete: migrate clients and remove
  val conflicting = Value(4, "conflicting") // no need to try with another executor: abort everything and revert
  val aborted = Value(5, "aborted") // the job terminated too early (everything has not been done), sending a [partial] result
  val unresolved = Value(6, "unresolved") // the job has terminated without sending the completion status to Perpetuo (should be considered as aborted)
  val completed = Value(7, "completed") // the completion status has been received, either succeeded or failed (per target)
  val unreachable = Value(8, "unreachable") // it's impossible to get information about the execution state (assumed permanently lost)
  val killed = Value(9, "killed") // the job has been successfully stopped from Perpetuo
}
