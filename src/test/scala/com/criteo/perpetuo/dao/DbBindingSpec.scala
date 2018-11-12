package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.DeploymentStatus
import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class Execution(executionTraceStates: Seq[ExecutionState.Value], targetStatuses: Seq[Status.Value])

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

  private def assessEffect(executions: Seq[Execution], planSteps: Seq[String], kind: Operation.Kind) =
    dbScenarios
      .insertEffect(executions, planSteps, kind)
      .flatMap { op =>
        val depReq = op.deploymentRequest
        getStatus(depReq).flatMap { case (runningStatus, runningKind) =>
          runningKind shouldEqual Some(kind)
          dbBinding.dbContext.db.run(dbBinding.closingOperationTrace(op))
            .flatMap(_ => getStatus(depReq))
            .map { case (closedStatus, closedKind) =>
              closedKind shouldEqual runningKind
              (runningStatus, closedStatus)
            }
        }
      }

  // to execute before the other tests:
  test("Compute a deployment status: deployment not started") {
    dbScenarios.deploymentPlans
      .map(_.head.deploymentRequest)
      .flatMap(getStatus) should
      eventually(be(DeploymentStatus.notStarted, None))
  }

  import DeploymentStatus._
  import ExecutionState._
  import Operation._
  import Status._

  test("The execution terminated unexpectedly") {
    assessEffect(
      Seq(
        Execution(Seq(aborted), Seq(notDone)),
        Execution(Seq(aborted), Seq())
      ),
      Seq("step-01"),
      deploy
    ) should eventually(be(inProgress, flopped))
  }

  test("Stopped while everything was working so far") {
    assessEffect(
      Seq(
        Execution(Seq(aborted), Seq(success)),
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-01"),
      deploy
    ) should eventually(be(inProgress, failed))
  }

  test("Failed revert marked as paused because incomplete") {
    assessEffect(
      Seq(
        Execution(Seq(aborted), Seq(notDone, hostFailure))
      ),
      Seq("step-02", "step-03"),
      revert
    ) should eventually(be(inProgress, paused))
  }

  test("Successful revert marked as paused because incomplete") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-02", "step-03"),
      revert
    ) should eventually(be(inProgress, paused))
  }

  test("All kinds of execution states") {
    assessEffect(
      Seq(
        Execution(Seq(aborted), Seq(success)),
        Execution(Seq(completed), Seq(success)),
        Execution(Seq(unreachable), Seq(success))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(inProgress, failed))
  }

  test("An execution is lost while the other one is successful") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(success)),
        Execution(Seq(unreachable), Seq(success))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(inProgress, failed))
  }

  test("Half flopped, half succeeded in one execution") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(notDone, success))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(inProgress, failed))
  }

  test("Deploy fails at the first step") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(productFailure))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(inProgress, failed))
  }

  test("Deploy paused because successful and incomplete (while there have been other operations on the deployment request)") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(inProgress, paused))
  }

  test("All kinds of target statuses") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(notDone)),
        Execution(Seq(completed), Seq(success, hostFailure)),
        Execution(Seq(completed), Seq(notDone, productFailure))
      ),
      Seq("step-12"),
      deploy
    ) should eventually(be(inProgress, failed))
  }

  test("Flopped") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(notDone)),
        Execution(Seq(unreachable), Seq(notDone)),
        Execution(Seq(aborted), Seq())
      ),
      Seq("step-12"),
      deploy
    ) should eventually(be(inProgress, flopped))
  }

  test("Half flopped, half succeeded in two executions") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(notDone)),
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-12"),
      deploy
    ) should eventually(be(inProgress, failed))
  }
}


class DbScenarios(dbBinding: DbBinding) extends DeploymentRequestInserter {
  override val dbContext: DbContext = dbBinding.dbContext

  import dbContext.profile.api._

  val deploymentPlans: Future[Iterable[DeploymentPlan]] =
    insertIntents(
      Map(
        "product-A" -> Seq(
          Seq("step-01", "step-02", "step-03"),
          Seq("step-11", "step-12")
        ),
        "product-B" -> Seq(
          Seq("step-21")
        ),
        "product-C" -> Seq()
      )
    )

  private val steps = deploymentPlans.map { plans =>
    val mapping = plans.flatMap(plan => plan.steps.map(step => step.name -> step))
    val ret = mapping.toMap
    assert(ret.size == mapping.size, "All declared steps must be unique in the test scenarios")
    ret
  }
  private val dummySpecIds = Stream.from(0).map(_ =>
    dbBinding.insertExecutionSpecification("foobar", Version(JsString("version"))).map(_.id)
  )
  private val dummyTargets = Iterator.from(100).map(n => TargetAtom(s"target-$n"))

  def insertEffect(executions: Seq[Execution], planSteps: Seq[String], kind: Operation.Kind): Future[OperationTrace] =
    Future
      .traverse(dummySpecIds.zip(executions)) { case (specId, execution) => specId.map((_, execution)) }
      .zip(steps)
      .flatMap { case (specIdsAndExecutions, stepsByName) =>
        val plan = planSteps.map(stepsByName)
        val requests = plan.map(_.deploymentRequest).toSet
        assert(requests.size == 1, "An operation must be bound to steps, which must all be bound to the same deployment request")
        val deploymentRequest = requests.head
        val q = dbBinding
          .insertOperationTrace(deploymentRequest, kind, "a.nonymous")
          .flatMap(newOp =>
            dbBinding
              .insertStepOperationXRefs(plan, newOp)
              .andThen(
                dbioTraverse(specIdsAndExecutions) { case (specId, execution) =>
                  dbBinding
                    .insertExecution(newOp.id, specId)
                    .flatMap { executionId =>
                      dbBinding.insertingExecutionTraces(
                        execution.executionTraceStates.map(state => ExecutionTraceRecord(None, executionId, "executor", None, state))
                      ).andThen(
                        dbBinding.updatingTargetStatuses(
                          executionId,
                          execution.targetStatuses.map(status => dummyTargets.next() -> TargetAtomStatus(status, "")).toMap
                        )
                      )
                    }
                }
              )
              .map(_ => newOp)
          )
        dbContext.db.run(q)
      }

  private def insertIntents(intents: Map[String, Seq[Seq[String]]]): Future[Iterable[DeploymentPlan]] =
    Future
      .traverse(intents) { case (productName, deploymentRequests) =>
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
