package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.DeploymentPlanStep
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private[dao] case class DeploymentPlanStepRecord(id: Option[Long],
                                                 deploymentRequestId: Long,
                                                 name: String,
                                                 targetExpression: String,
                                                 comment: String) // Not an `Option` because it's easier to consider that no comment <=> empty


trait DeploymentPlanStepBinder extends TableBinder {
  this: DeploymentRequestBinder with DbContextProvider =>

  import dbContext.driver.api._

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

  def insertDeploymentPlanStep(deploymentRequestId: Long, name: String, targetExpression: JsValue, comment: String): Future[DeploymentPlanStep] = {
    val record = DeploymentPlanStepRecord(None, deploymentRequestId, name, targetExpression.compactPrint, comment)
    dbContext.db
      .run((deploymentPlanStepQuery returning deploymentPlanStepQuery.map(_.id)) += record)
      .map(id => DeploymentPlanStep(id, deploymentRequestId, name, targetExpression, comment))
  }

  def findDeploymentPlanStepsByRequestId(deploymentRequestId: Long): Future[Seq[DeploymentPlanStep]] =
    dbContext.db
      .run(
        deploymentPlanStepQuery
          .filter(_.deploymentRequestId === deploymentRequestId)
          .result)
      .map(_.map(s => DeploymentPlanStep(s.id.get, s.deploymentRequestId, s.name, s.targetExpression.parseJson, s.comment)))
}
