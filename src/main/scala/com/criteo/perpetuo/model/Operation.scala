package com.criteo.perpetuo.model

object Operation extends Enumeration {
  type Kind = Value

  // these values must be stable, since they are persisted in the DB
  val deploy = Value(1)
  val revert = Value(2)

  /**
    * Reflect the fact that a Deploy and a Rollback do the exact same thing when given the right parameters:
    * the difference is only at the operation level, not at the execution one.
    */
  def executionKind(operation: Kind): String = (operation match {
    case `revert` => deploy
    case other => other
  }).toString
}
