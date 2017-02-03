package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import scala.concurrent.Await
import scala.concurrent.duration._


class Schema(val dbContext: DbContext)
  extends ExecutionTraceBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  val all = productQuery.schema ++ deploymentRequestQuery.schema ++ operationTraceQuery.schema ++ executionTraceQuery.schema

  def createTables(): Unit = {
    Await.result(dbContext.db.run(all.create), 2.seconds)
  }
}


@Singleton
class DbBinding @Inject()(val dbContext: DbContext)
  extends ExecutionTraceBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
    with DbContextProvider
