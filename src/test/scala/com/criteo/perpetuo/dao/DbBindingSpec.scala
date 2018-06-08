package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.DeploymentStatus
import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source


class DbBindingSpec extends TestHelpers with TestDb {
  private val dbScenarios = new DbScenarios(dbBinding)

  private def getStatus(deploymentRequest: DeepDeploymentRequest): Future[(DeploymentStatus.Value, Option[Operation.Kind])] =
    dbBinding
      .findDeploymentRequestsWithStatuses(Seq(Map("field" -> "id", "equals" -> deploymentRequest.id)), 10, 0)
      .map { seq =>
        seq.size shouldEqual 1
        val (depReq, status, kind) = seq.head
        depReq shouldEqual deploymentRequest
        (status, kind)
      }

  test("Compute a deployment status: deployment not started") {
    dbScenarios.deploymentPlans
      .map(_.head.deploymentRequest)
      .flatMap(getStatus) should
      eventually(be(DeploymentStatus.notStarted, None))
  }
}


class DbScenarios(dbBinding: DbBinding) extends DeploymentRequestInserter {
  override val dbContext: DbContext = dbBinding.dbContext

  case class Scenarios(intents: Map[String, List[List[String]]])

  object JsonConverters extends DefaultJsonProtocol {
    implicit val scenariosFormat: RootJsonFormat[Scenarios] = jsonFormat1(Scenarios)
  }


  import JsonConverters._
  import dbContext.profile.api._

  private val scenarios = Source.fromURL(getClass.getResource("db-scenarios.json")).mkString.parseJson.convertTo[Scenarios]
  val deploymentPlans: Future[Iterable[DeploymentPlan]] = insertIntent()

  private def insertIntent(): Future[Iterable[DeploymentPlan]] =
    Future
      .traverse(scenarios.intents) { case (productName, deploymentRequests) =>
        dbContext.db.run(productQuery += ProductRecord(None, productName)).flatMap(_ =>
          Future.traverse(deploymentRequests) { deploymentRequest =>
            val steps = deploymentRequest.map(ProtoDeploymentPlanStep(_, JsString("worldwide"), ""))
            insertDeploymentRequest(ProtoDeploymentRequest(productName, Version(JsString("42.0.1")), steps, "", "a.nonymous"))
          }
        )
      }
      .map(_.flatten)
}
