package com.criteo.perpetuo.dao


// todo: make it private[dao] again
case class StepOperationXRefRecord(deploymentPlanStepId: Long,
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
}
