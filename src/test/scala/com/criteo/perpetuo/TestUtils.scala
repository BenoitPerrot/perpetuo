package com.criteo.perpetuo

import com.criteo.perpetuo.auth.{Unrestricted, User}
import com.criteo.perpetuo.config.{AppConfigProvider, PluginLoader, Plugins}
import com.criteo.perpetuo.dao.{DbBinding, DbContext, DbContextProvider, TestingDbContextModule}
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.executors.{ExecutionTrigger, TriggeredExecutionFinder}
import com.criteo.perpetuo.engine.{Crankshaft, DeployFailed, DeployFlopped, Deployed, Engine, NotStarted, Outdated, Paused, RevertFailed, Reverted, TargetExpr, UnavailableAction}
import com.criteo.perpetuo.model._
import com.twitter.inject.Test
import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.scalatest.matchers.Matcher
import org.scalatest.mockito.MockitoSugar
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Awaitable, Future}
import scala.reflect.ClassTag


trait TestDb extends DbContextProvider {
  lazy val dbTestModule = new TestingDbContextModule(AppConfigProvider.config.getConfig("db"))
  lazy val dbContext: DbContext = dbTestModule.providesDbContext
  lazy val dbBinding = new DbBinding(dbContext)
}


trait TestHelpers extends Test {
  def asynchronouslyThrow[T <: Throwable : ClassTag](pattern: String): Matcher[Future[_]] =
    be(a[T]).and(fullyMatch.regex(pattern).compose((_: Throwable).getMessage)).compose(f => await(f.failed))

  def become[T](value: T): Matcher[Future[T]] = eventually(be(value))

  def eventually[T](matcher: Matcher[T]): Matcher[Future[T]] = matcher.compose(await)

  def await[T](a: Awaitable[T]): T = Await.result(a, 2.seconds)
}


trait SimpleScenarioTesting extends TestHelpers with TestDb with MockitoSugar {

  import dbContext.profile.api._

  private val knownProducts = mutable.Set[String]()
  private val loader = new PluginLoader(null)
  private val executionTrigger: ExecutionTrigger = mock[ExecutionTrigger]
  when(executionTrigger.executorType).thenReturn("testing")

  val plugins = new Plugins(loader)
  val executionFinder = new TriggeredExecutionFinder(loader)

  lazy val targetDispatcher: TargetDispatcher = new SingleTargetDispatcher(executionTrigger)

  lazy val crankshaft: Crankshaft = {
    when(executionTrigger.trigger(anyLong, anyString, any[Version], any[TargetExpr], anyString))
      .thenReturn(Future(triggerMock))
    new Crankshaft(dbBinding, plugins.resolver, targetDispatcher, plugins.listeners, executionFinder)
  }

  lazy val engine = new Engine(crankshaft, Unrestricted)

  protected def triggerMock: Option[String] = None

  def step(deploymentRequest: DeploymentRequest, operationCount: Option[Int], userName: String): Future[OperationTrace] =
    engine.step(User(userName), deploymentRequest.id, operationCount).map(_.get)

  def revert(deploymentRequest: DeploymentRequest, operationCount: Option[Int], initiatorName: String, defaultVersion: Option[Version]): Future[OperationTrace] =
    engine.revert(User(initiatorName), deploymentRequest.id, operationCount, defaultVersion).map(_.get)

  def findTargetsByExecution(executionId: Long): Future[Seq[TargetAtom]] =
    dbContext.db.run(dbBinding.targetStatusQuery.filter(_.executionId === executionId).map(_.targetAtom).result.map(_.map(_.toModel)))

  def closeOperation(operationTrace: OperationTrace, targetFinalStatus: Map[String, Status.Code] = Map()): Future[Seq[Long]] =
    tryCloseOperation(operationTrace, targetFinalStatus).map(
      _.map(_.getOrElse(throw UnavailableAction("An execution could not be updated: impossible transition")))
    )

  def tryCloseOperation(operationTrace: OperationTrace,
                        targetFinalStatus: Map[String, Status.Code] = Map()): Future[Seq[Option[Long]]] =
    crankshaft.dbBinding.findExecutionIdsByOperationTrace(operationTrace.id)
      .flatMap(executionIds =>
        Future.traverse(executionIds)(executionId =>
          findTargetsByExecution(executionId)
            .map(targets => targets
              .headOption
              .map(_ => targets.map(target => target -> targetFinalStatus.getOrElse(target.name, Status.success)).toMap)
              .getOrElse(targetFinalStatus.map { case (k, v) => TargetAtom(k) -> v })
            )
            .zip(crankshaft.dbBinding.findExecutionTraceIdsByExecution(executionId))
            .flatMap { case (statuses, executionTraceIds) =>
              val finalStatusMap = statuses.mapValues(TargetAtomStatus(_, ""))
              val executionState = ExecutionState.completed
              Future.traverse(executionTraceIds)(
                crankshaft.updateExecutionTrace(_, executionState, "", None, finalStatusMap)
                  .map(Some(_))
                  .recover { case _: UnavailableAction => None }
              )
            }
        )
      )
      .map(_.flatten)

  def request(productName: String, version: String, stepsTargets: Iterable[String]*): RequestTesting = {
    if (!knownProducts.contains(productName)) {
      await(crankshaft.upsertProduct(productName))
      knownProducts += productName
    }

    new RequestTesting(productName, version, stepsTargets)
  }

  class RequestTesting(productName: String, version: String, stepsTargets: Seq[Iterable[String]]) {
    private val deploymentRequest = await(crankshaft.createDeploymentRequest(ProtoDeploymentRequest(
      productName,
      Version(version.toJson),
      stepsTargets.zipWithIndex.map { case (target, i) => ProtoDeploymentPlanStep((i + 1).toString, target.toJson, "") },
      "",
      "de.ployer"
    )))
    private val currentState = Iterator.from(0)
    private var currentStep = 0

    def eligibleOperations: Future[Seq[Operation.Kind]] =
      crankshaft
        .assessDeploymentState(deploymentRequest)
        .map {
          case _: Outdated | _: Reverted => Seq()
          case _: NotStarted | _: DeployFlopped => Seq(Operation.deploy)
          case _: DeployFailed | _: Paused => Seq(Operation.deploy, Operation.revert)
          case _: Deployed | _: RevertFailed => Seq(Operation.revert)
          case _ => throw new RuntimeException("should not be there")
        }

    def startStep(): OperationTrace = {
      await(SimpleScenarioTesting.this.step(deploymentRequest, Some(currentState.next()), "s.tarter"))
    }

    def step(finalStatus: Status.Code = Status.success): OperationTrace = {
      val operationTrace = startStep()
      val targetStatus = stepsTargets(currentStep).map(_ -> finalStatus).toMap
      if (finalStatus == Status.success)
        currentStep += 1
      await(closeOperation(operationTrace, targetStatus))
      operationTrace
    }

    def revert(defaultVersion: String = "", finalStatus: Status.Code = Status.success): OperationTrace = {
      val targetStatus = stepsTargets.zipWithIndex.collect { case (target, index) if index < currentStep => target.map(_ -> finalStatus) }.flatten.toMap
      await(
        for {
          operationTrace <- SimpleScenarioTesting.this.revert(deploymentRequest, Some(currentState.next()), "r.everter", defaultVersion.headOption.map(_ => Version(defaultVersion.toJson)))
          _ <- closeOperation(operationTrace, targetStatus)
        } yield operationTrace
      )
    }
  }

}
