package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.Version

import scala.concurrent.Future


/**
  * An instance is supposed to be dedicated to an executor or a type of executor
  * and is able to trigger an execution on it.
  */
trait ExecutionTrigger {
  /**
    * Trigger a new execution.
    *
    * @return a possible log href to uniquely identify the execution, if available.
    */
  def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]]

  /**
    * The executorName should be stable because it's persisted in the DB and used
    * later to instantiate the right TriggeredExecution from a log href in order
    * to interact with an execution.
    */
  val executorName: String
}
