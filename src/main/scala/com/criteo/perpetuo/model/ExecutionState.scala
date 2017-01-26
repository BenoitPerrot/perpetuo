package com.criteo.perpetuo.model

object ExecutionState extends Enumeration {
  type ExecutionState = Value

  // these values must be stable, since they are persisted in the DB
  val pending = Value(1)
  val running = Value(2)
  val initFailed = Value(3) // the executor failed to do its job but another one *might* take over
  val conflicting = Value(4) // no need to try with another executor: abort everything and revert
  val aborted = Value(5) // the job run has been killed on purpose but sent a [partial] result
  val lost = Value(6) // the job run is terminated but the executor never reached Perpetuo
  val completed = Value(7) // the final status has been received, either succeeded or failed (per target)
}
