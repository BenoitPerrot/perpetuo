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

  private def createExecutions(productName: String, userInput: String = ""): (Int, Int, Seq[String]) = {
    val v = Version("\"8410\"")
    Await.result(
      for {
        _ <- schema.insert(productName)
        deploymentRequestId <- schema.insert(new DeploymentRequestAttrs(productName, v, "pune", "no comment", "r.equester", new java.sql.Timestamp(System.currentTimeMillis))).map(_.id)
        operationTraceId <- db.run((schema.operationTraceQuery returning schema.operationTraceQuery.map(_.id)) += OperationTraceRecord(None, deploymentRequestId, Operation.deploy, Some(Map()), "c.reator", new java.sql.Timestamp(0), None))
        _ <- db.run((schema.executionTraceQuery returning schema.executionTraceQuery.map(_.id)) += ExecutionTraceRecord(None, Some(operationTraceId), None, ExecutionState.completed, None))
        countBefore <- schema.countMissingExecutions()
        missed <- schema.createAllExecutions(userInput)
        countAfter <- schema.countMissingExecutions()
        specParams <- db.run(schema.executionQuery.filter(_.operationTraceId === operationTraceId).join(schema.executionSpecificationQuery).on(_.executionSpecificationId === _.id).map(_._2.specificParameters).result)
      } yield {
        missed.length shouldEqual countAfter
        (countBefore, countAfter, specParams)
      },
      10.minutes
    )
  }

  "Automatic creation of past executions and their specifications" should {
    "infer the right job name for products of a known type" in {
      createExecutions("perpetuo-app") shouldEqual(1, 0, Vector("""{"jobName":"deploy-to-marathon"}"""))
    }

    "not create executions for products of an unknown type" in {
      createExecutions("cod") shouldEqual(1, 1, Vector())
    }

    "take into account product types from user input" in {
      createExecutions("jedi", """{"jedi": "WHAT?"}""") shouldEqual(2, 1, Vector("""{"jobName":"deploy-to-WHAT?"}"""))
    }

    "not be messing things up with user input" in {
      createExecutions("angryboards-app", """{"jedi": "WHAT?"}""") shouldEqual(2, 1, Vector("""{"jobName":"deploy-to-marathon"}"""))
    }
  }
}
