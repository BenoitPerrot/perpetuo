package com.criteo.perpetuo.dao

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Operation, Version}
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
          operationTraceId <- db.run((schema.operationTraceQuery returning schema.operationTraceQuery.map(_.id)) += OperationTraceRecord(None, deploymentRequestId, Operation.deploy, Map(), "c.reator", new java.sql.Timestamp(0), None))
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
  }
}
