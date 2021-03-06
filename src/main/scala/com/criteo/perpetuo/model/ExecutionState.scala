package com.criteo.perpetuo.model

object ExecutionState extends Enumeration {
  type ExecutionState = Value

  // - the numeric values must be stable, since they are persisted in the DB
  // - the strings are used in the API, so clients must be migrated in case of change
  val pending = Value(1, "pending")
  val running = Value(2, "running") // todo: rename to started
  val initFailed = Value(3, "initFailed") // todo: obsolete: migrate clients and remove
  val conflicting = Value(4, "conflicting") // no need to try with another executor: abort everything and revert
  val aborted = Value(5, "aborted") // the job terminated too early (everything has not been done), sending a [partial] result
  val completed = Value(7, "completed") // the completion status has been received, either succeeded or failed (per target)
  val unreachable = Value(8, "unreachable") // it's impossible to get information about the execution state (assumed permanently lost)

  private val allOpenStates = List(pending, unreachable, running)
  val getPredecessors: Map[ExecutionState, List[ExecutionState]] = Map(
    pending -> List(),
    unreachable -> List(pending, running),
    running -> List(pending, unreachable),
    initFailed -> allOpenStates, // todo: remove
    conflicting -> allOpenStates,
    aborted -> allOpenStates,
    completed -> List(pending, running) // todo: remove pending once DREDD-725 is implemented
  )
  assert(values.equals(getPredecessors.keySet))
}
