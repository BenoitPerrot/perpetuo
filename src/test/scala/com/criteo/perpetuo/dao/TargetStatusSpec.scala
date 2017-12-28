package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


@RunWith(classOf[JUnitRunner])
class TargetStatusSpec extends FunSuite with ScalaFutures
  with TargetStatusBinder
  with ExecutionTraceBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
  with TestDb {

  import dbContext.driver.api._

  private def readStatuses: Future[String] =
    dbContext.db.run(targetStatusQuery.result).map(
      _.map(status => s"${status.targetAtom}: ${status.code} (${status.detail})").toList.sorted.mkString(", ")
    )

  test("Target statuses can be inserted and retrieved") {
    Await.result(
      for {
        product <- insertProduct("perpetuo-app")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"v42\""), "Moon", "That's one small step for man, one giant leap for mankind", "n.armstrong", new Timestamp(123456789)))
        deployOperationTrace <- insertOperationTrace(request.id, Operation.deploy, "n.armstrong")
        execSpec <- insertExecutionSpecification("{}", Version("\"456\""))
        execId <- insertExecution(deployOperationTrace.id, execSpec.id)
        _ <- insertTargetStatuses(execId, Map("Moon" -> TargetAtomStatus(Status.hostFailure, "Houston, we've got a problem")))
        targetStatuses <- readStatuses
        nbStatuses <- dbContext.db.run(targetStatusQuery.filter(_.executionId === execId).length.result)
      } yield {
        assert(targetStatuses == "Moon: hostFailure (Houston, we've got a problem)")
        assert(nbStatuses == 1)
      },
      1.second
    )
  }
}
