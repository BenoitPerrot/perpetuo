package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.UnprocessableIntent
import com.criteo.perpetuo.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class Schema(val dbContext: DbContext)
  extends DbContextProvider
    with ProductBinder
    with DeploymentRequestBinder
    with DeploymentPlanStepBinder
    with StepOperationXRefBinder
    with OperationTraceBinder
    with ExecutionBinder
    with ExecutionSpecificationBinder
    with TargetStatusBinder
    with ExecutionTraceBinder
    with LockBinder {

  import dbContext.profile.api._

  val all: dbContext.profile.DDL =
    productQuery.schema ++
      deploymentRequestQuery.schema ++
      deploymentPlanStepQuery.schema ++
      stepOperationXRefQuery.schema ++
      operationTraceQuery.schema ++
      executionSpecificationQuery.schema ++
      executionQuery.schema ++
      targetStatusQuery.schema ++
      executionTraceQuery.schema ++
      lockQuery.schema

  def createTables(): Unit = {
    Await.result(dbContext.db.run(all.create), 2.seconds)
  }
}


// todo: find a place for that trait; maybe one day there could be a plan inserter and an effect inserter, and they would end up in a same dedicated module?
trait DeploymentRequestInserter
  extends DbContextProvider
    with ProductBinder
    with DeploymentRequestBinder
    with DeploymentPlanStepBinder {

  import dbContext.profile.api._

  def insertDeploymentRequest(d: DeploymentRequestAttrs): Future[DeepDeploymentRequest] = {
    findProductByName(d.productName).map(_.getOrElse {
      throw UnprocessableIntent(s"Unknown product `${d.productName}`")
    }).flatMap { product =>
      val q = (for {
        deploymentRequestId <-
          deploymentRequestQuery.returning(deploymentRequestQuery.map(_.id)) +=
            DeploymentRequestRecord(None, product.id, d.version, d.target, d.comment, d.creator, d.creationDate)
        _ <-
          deploymentPlanStepQuery ++=
            d.plan.map(step =>
              DeploymentPlanStepRecord(None, deploymentRequestId, step.name, step.targetExpression.compactPrint, step.comment)
            )
      } yield deploymentRequestId).transactionally

      dbContext.db.run(q).map { id =>
        val ret = DeepDeploymentRequest(id, product, d.version, d.target, d.comment, d.creator, d.creationDate)
        ret.copyParsedTargetCacheFrom(d)
        ret
      }
    }
  }
}
