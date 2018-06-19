package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{DeploymentPlanStep, OperationTrace}
import com.google.common.annotations.VisibleForTesting
import slick.sql.FixedSqlAction

import scala.concurrent.Future


private[dao] case class StepOperationXRefRecord(deploymentPlanStepId: Long,
                                                operationTraceId: Long)


trait StepOperationXRefBinder extends TableBinder {
  this: DeploymentPlanStepBinder with OperationTraceBinder with DbContextProvider =>

  import dbContext.profile.api._

  class StepOperationXRefTable(tag: Tag) extends Table[StepOperationXRefRecord](tag, "step_operation_xref") {
    def deploymentPlanStepId = column[Long]("deployment_plan_step_id")
    protected def dpsFk = foreignKey(deploymentPlanStepId, deploymentPlanStepQuery)(_.id)

    def operationTraceId = column[Long]("operation_trace_id")
    protected def otFk = foreignKey(operationTraceId, operationTraceQuery)(_.id)

    protected def pk = primaryKey((deploymentPlanStepId, operationTraceId))

    def * = (deploymentPlanStepId, operationTraceId) <> (StepOperationXRefRecord.tupled, StepOperationXRefRecord.unapply)
  }

  val stepOperationXRefQuery = TableQuery[StepOperationXRefTable]

  def insertStepOperationXRefs(steps: Iterable[DeploymentPlanStep], operationTrace: OperationTrace): FixedSqlAction[Option[Int], NoStream, Effect.Write] =
    stepOperationXRefQuery ++= steps.map(dps => StepOperationXRefRecord(dps.id, operationTrace.id))

  @VisibleForTesting
  protected def findStepOperationXRefs(deploymentPlanStep: DeploymentPlanStep): Future[Seq[StepOperationXRefRecord]] =
    dbContext.db.run(stepOperationXRefQuery.filter(_.deploymentPlanStepId === deploymentPlanStep.id).result)

  @VisibleForTesting
  protected def findStepOperationXRefs(operationTrace: OperationTrace): Future[Seq[StepOperationXRefRecord]] =
    dbContext.db.run(stepOperationXRefQuery.filter(_.operationTraceId === operationTrace.id).result)
}
