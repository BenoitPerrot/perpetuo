package com.criteo.perpetuo.dao

import com.criteo.perpetuo.config.TestConfig
import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Execution(executionTraceStates: Seq[ExecutionState.Value], targetStatuses: Seq[Status.Value])

class DbBindingSpec extends TestHelpers with TestDb {
  private val dbScenarios = new DbScenarios(dbBinding)
  private val fuelFilter = new FuelFilter(TestConfig, dbBinding)
  private val crankshaft = new Crankshaft(dbBinding, fuelFilter, null, Seq(), null, null)

  private def assessEffect(executions: Seq[Execution], planSteps: Seq[String], kind: Operation.Kind) =
    dbScenarios
      .insertEffect(executions, planSteps, kind)
      .flatMap { op =>
        val depReq = op.deploymentRequest
        crankshaft
          .assessDeploymentState(depReq)
          .flatMap { runningState =>
            dbBinding.dbContext.db.run(dbBinding.closingOperationTrace(op))
              .flatMap(_ => crankshaft.assessDeploymentState(depReq))
              .map { closedState =>
                runningState shouldBe an[InProgressState]
                closedState
              }
          }
      }

  // to execute before the other tests:
  test("Compute a deployment status: deployment not started") {
    dbScenarios.deploymentPlans
      .map(_.head.deploymentRequest)
      .flatMap(crankshaft.assessDeploymentState) should
      eventually(be(a[NotStarted]))
  }

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
    ) should eventually(be(a[DeployFlopped]))
  }

  test("Stopped while everything was working so far") {
    assessEffect(
      Seq(
        Execution(Seq(aborted), Seq(success)),
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-01"),
      deploy
    ) should eventually(be(a[DeployFailed]))
  }

  test("Failed revert marked as such") {
    assessEffect(
      Seq(
        Execution(Seq(aborted), Seq(notDone, hostFailure))
      ),
      Seq("step-02", "step-03"),
      revert
    ) should eventually(be(a[RevertFailed]))
  }

  test("Successful revert marked as such") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-02", "step-03"),
      revert
    ) should eventually(be(a[Reverted]))
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
    ) should eventually(be(a[DeployFailed]))
  }

  test("An execution is lost while the other one is successful") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(success)),
        Execution(Seq(unreachable), Seq(success))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(a[DeployFailed]))
  }

  test("Half flopped, half succeeded in one execution") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(notDone, success))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(a[DeployFailed]))
  }

  test("Deploy fails at the first step") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(productFailure))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(a[DeployFailed]))
  }

  test("Deploy paused because successful and incomplete (while there have been other operations on the deployment request)") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-11"),
      deploy
    ) should eventually(be(a[Paused]))
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
    ) should eventually(be(a[DeployFailed]))
  }

  test("Half flopped, half succeeded in two executions") {
    assessEffect(
      Seq(
        Execution(Seq(completed), Seq(notDone)),
        Execution(Seq(completed), Seq(success))
      ),
      Seq("step-12"),
      deploy
    ) should eventually(be(a[DeployFailed]))
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
          deploymentRequests.foldLeft[Future[Seq[DeploymentPlan]]](Future.successful(Seq()))((f, planDesc) =>
            f.flatMap { plans =>
              val steps = planDesc.map(ProtoDeploymentPlanStep(_, JsString("worldwide"), ""))
              insertDeploymentRequest(ProtoDeploymentRequest(productName, Version(JsString("42.0.1")), steps, "", "a.nonymous"))
                .map(plans.+:(_))
            }
          )
        )
      }
      .map(_.flatten)

  private def dbioTraverse[A, B, E <: Effect](in: Iterable[A])(f: A => DBIOAction[B, NoStream, E]): DBIOAction[Iterable[B], NoStream, E] =
    DBIO.sequence(in.map(f))
}
