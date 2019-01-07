package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.model.Version

import scala.concurrent.Future


/**
  * An instance is supposed to be dedicated to an executor or a type of executor
  * and is able to trigger an execution on it.
  *
  * It is advised that any concrete class of this type have a constructor taking
  * either no parameter or a Config as single parameter in order to be usable
  * by configuration only, in conjunction with the "singleExecutor" dispatcher.
  */
trait ExecutionTrigger {
  /**
    * Trigger a new execution.
    *
    * @return a possible href to uniquely identify the execution, if available.
    */
  def trigger(execTraceId: Long, productName: String, version: Version, target: TargetAtomSet, initiator: String): Future[Option[String]]

  /**
    * The executorName should be stable because it's persisted in the DB and used
    * later to instantiate the right TriggeredExecution from a href in order
    * to interact with an execution.
    */
  val executorType: String
}
