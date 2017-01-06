package com.criteo.perpetuo.dao.enums


object Operation extends Enumeration {
  type Operation = Value

  // these values must be stable, since they are persisted in the DB
  val deploy = Value(1)
  val revert = Value(2)
}
