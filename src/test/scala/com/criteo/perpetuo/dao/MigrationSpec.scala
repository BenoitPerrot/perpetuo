package com.criteo.perpetuo.dao

import java.util.Calendar

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, ExecutionState, Operation, Version}
import com.twitter.inject.Test

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MigrationSpec extends Test with TestDb {

  val schema = new Schema(dbContext)

  import dbContext.driver.api._

  val db = dbContext.db

  "Operation traces" should {

    "obtain missing creation dates" in {
      val now = new java.sql.Timestamp(System.currentTimeMillis)
      Await.result(
        for {
          _ <- schema.insert("pirhana")
          deploymentRequestId <- schema.insert(new DeploymentRequestAttrs("pirhana", Version("\"4242\""), "paris", "no comment", "r.equester", now)).map(_.id)
          operationTraceId <- db.run((schema.operationTraceQuery returning schema.operationTraceQuery.map(_.id)) += OperationTraceRecord(None, deploymentRequestId, Operation.deploy, Some(Map()), "c.reator", new java.sql.Timestamp(0), None))
          countBefore <- schema.countOperationTracesMissingCreationDate()
          _ <- schema.setOperationTracesMissingCreationDate()
          countAfter <- schema.countOperationTracesMissingCreationDate()
          operationTraceAfter <- db.run((schema.operationTraceQuery filter (_.id === operationTraceId)).result).map(_.head)
        } yield {
          (countBefore, countAfter, operationTraceAfter.creationDate)
        },
        2.second
      ) shouldEqual(1, 0, now)
    }

    "obtain missing closing dates" in {
      val now = new java.sql.Timestamp(System.currentTimeMillis)
      val later = {
        val cal = Calendar.getInstance()
        cal.setTime(now)
        cal.add(Calendar.MINUTE, 1)
        new java.sql.Timestamp(cal.getTime.getTime)
      }

      Await.result(
        for {
          _ <- schema.insert("tuna")
          deploymentRequestId <- schema.insert(new DeploymentRequestAttrs("tuna", Version("\"22\""), "nice", "no comment", "r.equester", now)).map(_.id)
          operationTraceId <- db.run((schema.operationTraceQuery returning schema.operationTraceQuery.map(_.id)) += OperationTraceRecord(None, deploymentRequestId, Operation.deploy, Some(Map()), "c.reator", new java.sql.Timestamp(0), None))
          countBefore <- schema.countOperationTracesMissingClosingDate()
          _ <- schema.setOperationTracesMissingClosingDate()
          countAfter <- schema.countOperationTracesMissingClosingDate()
          operationTraceAfter <- db.run((schema.operationTraceQuery filter (_.id === operationTraceId)).result).map(_.head)
        } yield {
          (countBefore, countAfter, operationTraceAfter.closingDate)
        },
        2.second
      ) shouldEqual (2, 0, Some(later))
    }
  }

  "Execution traces" should {
    "obtain missing links to executions" in {
      val v = Version("\"8410\"")
      Await.result(
        for {
          _ <- schema.insert("cod")
          deploymentRequestId <- schema.insert(new DeploymentRequestAttrs("cod", v, "pune", "no comment", "r.equester", new java.sql.Timestamp(System.currentTimeMillis))).map(_.id)
          operationTraceId <- db.run((schema.operationTraceQuery returning schema.operationTraceQuery.map(_.id)) += OperationTraceRecord(None, deploymentRequestId, Operation.deploy, Some(Map()), "c.reator", new java.sql.Timestamp(0), None))
          executionTraceId <- db.run((schema.executionTraceQuery returning schema.executionTraceQuery.map(_.id)) += ExecutionTraceRecord(None, Some(operationTraceId), None, ExecutionState.completed, None))
          executionSpecId <- db.run((schema.executionSpecificationQuery returning schema.executionSpecificationQuery.map(_.id)) += ExecutionSpecificationRecord(None, Some(v), ""))
          executionId <- db.run((schema.executionQuery returning schema.executionQuery.map(_.id)) += ExecutionRecord(None, operationTraceId, executionSpecId))
          countBefore <- schema.countExecutionTracesMissingLinkToExecution()
          _ <- schema.addExecutionTracesMissingLinkToExecution()
          countAfter <- schema.countExecutionTracesMissingLinkToExecution()
        } yield {
          (countBefore, countAfter)
        },
        2.hours
      ) shouldEqual (1, 0)
    }
  }
}
