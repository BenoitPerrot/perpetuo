package com.criteo.perpetuo.dao

import slick.driver.JdbcProfile

import scala.concurrent.Await
import scala.concurrent.duration._


class Schema(val profile: JdbcProfile)
  extends ExecutionTraceBinder with OperationTraceBinder with DeploymentRequestBinder
    with ProfileProvider {

  import profile.api._

  def createTables(db: Database): Unit = {
    val schema = DBIO.seq(
      deploymentRequestQuery.schema.create,
      operationTraceQuery.schema.create,
      executionTraceQuery.schema.create
    )
    Await.result(db.run(schema), 2.seconds)
  }
}
