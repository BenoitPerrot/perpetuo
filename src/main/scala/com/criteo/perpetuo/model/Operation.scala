package com.criteo.perpetuo.model

object Operation extends Enumeration {
  type Kind = Value

  // - the numeric values must be stable, since they are persisted in the DB
  // - the strings are used in the API, so clients must be migrated in case of change
  val deploy = Value(1, "deploy")
  val revert = Value(2, "revert")
}
