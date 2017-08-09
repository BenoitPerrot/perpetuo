package com.criteo.perpetuo.model

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


@RunWith(classOf[JUnitRunner])
class TargetStatusSpec extends FunSuite with ScalaFutures
  with TargetStatusBinder
  with ExecutionTraceBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
  with TestDb {

  import dbContext.driver.api._

  test("Target statuses can be inserted and retrieved") {
    Await.result(
      for {
        product <- insertProduct("perpetuo-app")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("v42"), "Moon", "That's one small step for man, one giant leap for mankind", "n.armstrong", new Timestamp(123456789)))
        deployOperationTrace <- insertOperationTrace(request.id, Operation.deploy, "n.armstrong")
        execSpec <- insertExecutionSpecification("{}", Version("456"))
        execId <- insertExecution(deployOperationTrace.id, execSpec.id)
        _ <- insertTargetStatuses(execId, Map("Moon" -> TargetAtomStatus(Status.hostFailure, "Houston, we've got a problem")))
        targetStatuses <- dbContext.db.run(targetStatusQuery.result)
      } yield {
        assert(targetStatuses.length == 1)
        assert(targetStatuses.head.executionId == execId)
        assert(targetStatuses.head.targetAtom == "Moon")
        assert(targetStatuses.head.code == Status.hostFailure)
        assert(targetStatuses.head.detail == "Houston, we've got a problem")
      },
      1.second
    )
  }
}
