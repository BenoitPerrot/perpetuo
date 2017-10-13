package com.criteo.perpetuo.model

object Operation extends Enumeration {
  type Kind = Value

  // these values must be stable, since they are persisted in the DB
  val deploy = Value(1)
  val revert = Value(2)
}
