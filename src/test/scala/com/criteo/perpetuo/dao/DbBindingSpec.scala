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

  private def getStatus(deploymentRequest: DeploymentRequest): Future[(DeploymentStatus.Value, Option[Operation.Kind])] =
    dbBinding
      .findDeploymentRequestsWithStatuses(Seq(Map("field" -> "id", "equals" -> deploymentRequest.id)), 10, 0)
      .map { seq =>
        seq.size shouldEqual 1
        val (depPlan, status, kind) = seq.head
        depPlan.deploymentRequest shouldEqual deploymentRequest
        (status, kind)
      }

  // to execute before the other tests:
  test("Compute a deployment status: deployment not started") {
    dbScenarios.deploymentPlans
      .map(_.head.deploymentRequest)
      .flatMap(getStatus) should
      eventually(be(DeploymentStatus.notStarted, None))
  }

  // automatic test generation from scenarios:
  dbScenarios.effects.toStream.foreach(effect =>
    test(s"Compute a deployment status: ${effect.scenarioName}") {
      dbScenarios.insertEffect(effect)
        .flatMap { op =>
          val depReq = op.deploymentRequest
          getStatus(depReq).flatMap { case (runningStatus, kind) =>
            kind shouldEqual Some(effect.kind)
            dbBinding.dbContext.db.run(dbBinding.closingOperationTrace(op))
              .flatMap(_ => getStatus(depReq))
              .map { case (closedStatus, k) =>
                k shouldEqual kind
                (runningStatus, closedStatus)
              }
          }
        } should
        eventually(be(DeploymentStatus.inProgress, DeploymentStatus.withName(effect.deploymentStatus)))
    }
  )
}


class DbScenarios(dbBinding: DbBinding) extends DeploymentRequestInserter {
  override val dbContext: DbContext = dbBinding.dbContext

  case class Execution(executionTraceStates: List[String], targetStatuses: List[String])

  case class OperationEffect(scenarioName: String, deploymentStatus: String, planSteps: List[String], executions: List[Execution], private val operation: Option[String]) {
    def kind: Operation.Kind = operation.map(Operation.withName).getOrElse(Operation.deploy)
  }

  case class Scenarios(intents: Map[String, List[List[String]]], effects: List[OperationEffect])

  object JsonConverters extends DefaultJsonProtocol {
    implicit val executionFormat: RootJsonFormat[Execution] = jsonFormat2(Execution)
    implicit val operationFormat: RootJsonFormat[OperationEffect] = jsonFormat5(OperationEffect)
    implicit val scenariosFormat: RootJsonFormat[Scenarios] = jsonFormat2(Scenarios)
  }


  import JsonConverters._
  import dbContext.profile.api._

  private val scenarios = Source.fromURL(getClass.getResource("db-scenarios.json")).mkString.parseJson.convertTo[Scenarios]
  val deploymentPlans: Future[Iterable[DeploymentPlan]] = insertIntent()
  val effects: List[OperationEffect] = scenarios.effects

  private val steps = deploymentPlans.map { plans =>
    val mapping = plans.flatMap(plan => plan.steps.map(step => step.name -> step))
    val ret = mapping.toMap
    assert(ret.size == mapping.size, "All declared steps must be unique in the test scenarios")
    ret
  }
  private val dummySpecIds = Stream.from(0).map(_ =>
    dbBinding.insertExecutionSpecification("foobar", Version(JsString("version"))).map(_.id)
  )
  private val dummyTargets = Iterator.from(100).map(n => s"target-$n")

  def insertEffect(effect: OperationEffect): Future[OperationTrace] =
    Future
      .traverse(dummySpecIds.zip(effect.executions)) { case (specId, execution) => specId.map((_, execution)) }
      .zip(steps)
      .flatMap { case (specIdsAndExecutions, stepsByName) =>
        val plan = effect.planSteps.map(stepsByName)
        val requests = plan.map(_.deploymentRequest).toSet
        assert(requests.size == 1, "An operation must be bound to steps, which must all be bound to the same deployment request")
        val deploymentRequest = requests.head
        val q = dbBinding
          .insertOperationTrace(deploymentRequest, effect.kind, "a.nonymous")
          .flatMap(newOp =>
            dbBinding
              .insertStepOperationXRefs(plan, newOp)
              .andThen(
                dbioTraverse(specIdsAndExecutions) { case (specId, execution) =>
                  dbBinding
                    .insertExecution(newOp.id, specId)
                    .flatMap { executionId =>
                      dbBinding.insertingExecutionTraces(
                        executionId,
                        execution.executionTraceStates.map(state => ExecutionTraceRecord(None, executionId, None, ExecutionState.withName(state)))
                      ).andThen(
                        dbBinding.updatingTargetStatuses(
                          executionId,
                          execution.targetStatuses.map(status => dummyTargets.next() -> TargetAtomStatus(Status.withName(status), "")).toMap
                        )
                      )
                    }
                }
              )
              .map(_ => newOp)
          )
        dbContext.db.run(q)
      }

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

  private def dbioTraverse[A, B, E <: Effect](in: Iterable[A])(f: A => DBIOAction[B, NoStream, E]): DBIOAction[Iterable[B], NoStream, E] =
    DBIO.sequence(in.map(f))
}
