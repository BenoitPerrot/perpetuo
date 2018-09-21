package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.UnprocessableIntent
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
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
            .into((_, id) => DeploymentRequest(id, product, r.version, r.comment, r.creator, r.creationDate))
            .+=(
              DeploymentRequestRecord(None, product.id, r.version, r.comment, r.creator, r.creationDate)
            )
        planSteps <-
          deploymentPlanStepQuery
            .returning(deploymentPlanStepQuery.map(_.id))
            .into((planStep, id) => DeploymentPlanStep(id, deploymentRequest, planStep.name, targetExpressions(planStep.targetExpression), planStep.comment.toString))
            .++=(
              r.plan.map(step => DeploymentPlanStepRecord(None, deploymentRequest.id, step.name, step.targetExpression.compactPrint, step.comment))
            )
      } yield (deploymentRequest, planSteps)).transactionally

      dbContext.db.run(q).map { case (depReq, planSteps) =>
        DeploymentPlan(depReq, planSteps)
      }
    }
  }
}


trait EffectInserter
  extends DbContextProvider
    with ProductBinder
    with DeploymentRequestBinder
    with DeploymentPlanStepBinder
    with OperationTraceBinder
    with StepOperationXRefBinder
    with ExecutionSpecificationBinder
    with ExecutionBinder
    with ExecutionTraceBinder
    with TargetStatusBinder {

  import dbContext.profile.api._

  // fixme: type the targets to differentiate atomic from other ones in order to always create atomic targets
  //        instead of taking the boolean as parameter (to be done later, since it's not trivial)
  def insertingEffect(deploymentRequest: DeploymentRequest,
                      deploymentPlanSteps: Iterable[DeploymentPlanStep],
                      operation: Operation.Kind,
                      userName: String,
                      specAndInvocations: Iterable[(ExecutionSpecification, Vector[(ExecutionTrigger, TargetExpr)])],
                      createTargetStatuses: Boolean = false): DBIOrw[(OperationTrace, Iterable[(Long, Version, TargetExpr, ExecutionTrigger)])] =
    insertOperationTrace(deploymentRequest, operation, userName)
      .flatMap { newOp =>
        insertStepOperationXRefs(deploymentPlanSteps, newOp).andThen(
          DBIO
            .sequence( // in sequence to be able to put all these SQL queries in the same transaction
              specAndInvocations.map { case (spec, invocations) =>
                insertExecution(newOp.id, spec.id)
                  .flatMap { executionId =>
                    // create as many traces as needed, all at the same time
                    val ret = insertingExecutionTraces(executionId, invocations.length)
                    if (createTargetStatuses)
                      updatingTargetStatuses(
                        executionId,
                        invocations
                          .toStream
                          .flatMap { case (_, target) => target }
                          .map(TargetAtom(_) -> TargetAtomStatus(Status.notDone, ""))
                          .toMap
                      )
                        .andThen(ret)
                    else
                      ret
                  }
                  .map(_.zip(invocations).map { case (execTraceId, (trigger, target)) => (execTraceId, spec.version, target, trigger) })
              }
            )
            .map(effects => (newOp, effects.flatten))
        )
      }
}
