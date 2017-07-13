package com.criteo.perpetuo.engine

import com.twitter.inject.Test
import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.model.ExecutionState

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

class EngineSpec extends Test with TestDb {

  private val engine = new Engine(new DbBinding(dbContext))

  "Engine" should {
    "keep track of open executions for an operation" in {
      Await.result(
        for {
          product <- engine.insertProduct("human")
          deploymentRequestId <- engine.createDeploymentRequest("robert", s"""{"productName":"${product.name}","version":"42","target":["moon","mars"]}""", immediateStart = false).map(_ ("id").toString.toLong)
          _ <- engine.startDeploymentRequest(deploymentRequestId, "ignace")
          operationTraces <- engine.findOperationTracesByDeploymentRequest(deploymentRequestId).map(_.get)
          operationTraceId = operationTraces.head.id
          hasOpenExecutionBefore <- engine.dbBinding.hasOpenExecutionTracesForOperation(operationTraceId)
          executionTrace <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).map(_.head)
          _ <- engine.updateExecutionTrace(executionTrace.id, ExecutionState.completed, "", Map("moon" -> Map("code" -> "success", "detail" -> "no surprise"), "mars" -> Map("code" -> "hostFailure", "detail" -> "no surprise")))
          hasOpenExecutionAfter <- engine.dbBinding.hasOpenExecutionTracesForOperation(operationTraceId)
          operationClosingSucceeded <- engine.dbBinding.closeOperationTrace(operationTraceId)
        } yield {
          hasOpenExecutionBefore && !hasOpenExecutionAfter && !operationClosingSucceeded
        },
        2.second
      ) shouldBe true
    }
  }

}
