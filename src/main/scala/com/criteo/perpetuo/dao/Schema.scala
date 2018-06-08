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

  // todo: rename it to insertIntent or insertDeploymentIntent, and likely rename the return type?
  def insertDeploymentRequest(r: ProtoDeploymentRequest): Future[DeploymentPlan] = {
    val targetExpressions = r.plan.map(step => step.targetExpression.compactPrint -> step.targetExpression).toMap // avoid useless JSON parsing
    findProductByName(r.productName).map(_.getOrElse {
      throw UnprocessableIntent(s"Unknown product `${r.productName}`")
    }).flatMap { product =>
      val q = (for {
        deploymentRequest <-
          deploymentRequestQuery
            .returning(deploymentRequestQuery.map(_.id))
            .into((_, id) => DeepDeploymentRequest(id, product, r.version, r.target, r.comment, r.creator, r.creationDate))
            .+=(
              DeploymentRequestRecord(None, product.id, r.version, r.target, r.comment, r.creator, r.creationDate)
            )
        planSteps <-
          deploymentPlanStepQuery
            .returning(deploymentPlanStepQuery.map(_.id))
            .into((planStep, id) => DeploymentPlanStep(id, deploymentRequest, planStep.name, targetExpressions(planStep.targetExpression), planStep.comment))
            .++=(
              r.plan.map(step => DeploymentPlanStepRecord(None, deploymentRequest.id, step.name, step.targetExpression.compactPrint, step.comment))
            )
      } yield (deploymentRequest, planSteps)).transactionally

      dbContext.db.run(q).map { case (depReq, planSteps) =>
        depReq.copyParsedTargetCacheFrom(r)
        DeploymentPlan(depReq, planSteps)
      }
    }
  }
}
