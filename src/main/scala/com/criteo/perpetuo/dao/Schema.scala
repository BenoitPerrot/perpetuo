package com.criteo.perpetuo.dao

import com.criteo.perpetuo.app.DbContext

import scala.concurrent.Await
import scala.concurrent.duration._


class Schema(val dbContext: DbContext)
  extends ExecutionTraceBinder with OperationTraceBinder with DeploymentRequestBinder
    with DbContextProvider {

  import dbContext.driver.api._

  def createTables(): Unit = {
    val schema = DBIO.seq(
      deploymentRequestQuery.schema.create,
      operationTraceQuery.schema.create,
      executionTraceQuery.schema.create
    )
    Await.result(dbContext.db.run(schema), 2.seconds)
  }
}
