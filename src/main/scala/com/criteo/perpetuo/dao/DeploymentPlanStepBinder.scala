package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model._
import com.google.common.annotations.VisibleForTesting
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// todo: make it private[dao] again
case class DeploymentPlanStepRecord(id: Option[Long],
                                    deploymentRequestId: Long,
                                    name: String,
                                    targetExpression: String,
                                    comment: String) // Not an `Option` because it's easier to consider that no comment <=> empty


trait DeploymentPlanStepBinder extends TableBinder {
  this: DeploymentRequestBinder with DbContextProvider =>

  import dbContext.profile.api._

  class DeploymentPlanStepTable(tag: Tag) extends Table[DeploymentPlanStepRecord](tag, "deployment_plan_step") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def name = column[String]("name", O.SqlType(s"nvarchar(1024)"))

    def targetExpression = column[String]("target_expression", O.SqlType("nvarchar(8000)"))

    def comment = column[String]("comment", O.SqlType("nvarchar(4000)"))

    def * = (id.?, deploymentRequestId, name, targetExpression, comment) <> (DeploymentPlanStepRecord.tupled, DeploymentPlanStepRecord.unapply)
  }

  val deploymentPlanStepQuery = TableQuery[DeploymentPlanStepTable]

  // todo: add tests to cover the retrieval of plan steps from the insertion of full deployment requests, then remove this method:

  @VisibleForTesting
  protected def insertDeploymentPlanStep(deploymentRequest: DeepDeploymentRequest, step: ProtoDeploymentPlanStep): Future[DeploymentPlanStep] = {
    val record = DeploymentPlanStepRecord(None, deploymentRequest.id, step.name, step.targetExpression.compactPrint, step.comment)
    dbContext.db
      .run((deploymentPlanStepQuery returning deploymentPlanStepQuery.map(_.id)) += record)
      .map(id => DeploymentPlanStep(id, deploymentRequest, step.name, step.targetExpression, step.comment))
  }

  def findDeploymentPlan(deploymentRequest: DeepDeploymentRequest): Future[DeploymentPlan] =
    dbContext.db
      .run(deploymentPlanStepQuery.filter(_.deploymentRequestId === deploymentRequest.id).result)
      .map(steps => DeploymentPlan(deploymentRequest, steps.map(s => DeploymentPlanStep(s.id.get, deploymentRequest, s.name, s.targetExpression.parseJson, s.comment))))
}
