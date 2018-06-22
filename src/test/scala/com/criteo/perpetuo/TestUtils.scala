package com.criteo.perpetuo

import com.criteo.perpetuo.config.{AppConfigProvider, PluginLoader, Plugins}
import com.criteo.perpetuo.dao.{DbBinding, DbContext, DbContextProvider, TestingDbContextModule}
import com.criteo.perpetuo.engine.dispatchers.SingleTargetDispatcher
import com.criteo.perpetuo.engine.executors.{DummyExecutionTrigger, TriggeredExecutionFinder}
import com.criteo.perpetuo.engine.{Crankshaft, TargetExpr, UnavailableAction}
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
    beLike[T](pattern).compose(f => await(f.failed))

  def beLike[T <: Throwable : ClassTag](pattern: String): Matcher[Throwable] =
    be(a[T]).and(fullyMatch.regex(pattern).compose(_.getMessage))

  def become[T](value: T): Matcher[Future[T]] = eventually(be(value))

  def eventually[T](matcher: Matcher[T]): Matcher[Future[T]] = matcher.compose(await)

  def await[T](a: Awaitable[T]): T = Await.result(a, 2.seconds)
}


trait SimpleScenarioTesting extends TestHelpers with TestDb with MockitoSugar {
  private val knownProducts = mutable.Set[String]()
  private val loader = new PluginLoader(null)
  private val executionTrigger: DummyExecutionTrigger = mock[DummyExecutionTrigger]
  val plugins = new Plugins(loader)
  val executionFinder = new TriggeredExecutionFinder(loader)
  lazy val crankshaft: Crankshaft = {
    when(executionTrigger.trigger(anyInt, anyString, any[Version], any[TargetExpr], anyString))
      .thenReturn(Future(triggerMock))
    new Crankshaft(dbBinding, plugins.resolver, new SingleTargetDispatcher(executionTrigger), plugins.listeners, executionFinder)
  }

  protected def triggerMock: Option[String] = None

  def closeOperation(operationTrace: OperationTrace,
                     targetFinalStatus: Map[String, Status.Code] = Map(),
                     initFailed: Boolean = false): Future[Seq[Long]] =
    tryCloseOperation(operationTrace, targetFinalStatus, initFailed).map(
      _.map(_.getOrElse(throw new AssertionError("An execution could not be updated: impossible transition")))
    )

  def tryCloseOperation(operationTrace: OperationTrace,
                        targetFinalStatus: Map[String, Status.Code] = Map(),
                        initFailed: Boolean = false): Future[Seq[Option[Long]]] =
    crankshaft.dbBinding.findExecutionIdsByOperationTrace(operationTrace.id)
      .flatMap(executionIds =>
        Future.traverse(executionIds)(executionId =>
          targetFinalStatus.headOption
            .map(_ => Future.successful(targetFinalStatus))
            .getOrElse(crankshaft.dbBinding.findTargetsByExecution(executionId).map(_.map(_ -> Status.success).toMap))
            .zip(crankshaft.dbBinding.findExecutionTraceIdsByExecution(executionId))
            .flatMap { case (statusMap, executionTraceIds) =>
              val finalStatusMap = statusMap.mapValues(TargetAtomStatus(_, ""))
              val executionState = if (initFailed) ExecutionState.initFailed else ExecutionState.completed
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
      await(crankshaft.insertProductIfNotExists(productName))
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

    def startStep(): DeepOperationTrace = {
      await(crankshaft.step(deploymentRequest, Some(currentState.next()), "s.tarter"))
    }

    def step(finalStatus: Status.Code = Status.success): DeepOperationTrace = {
      val operationTrace = startStep()
      val targetStatus = stepsTargets(currentStep).map(_ -> finalStatus).toMap
      if (finalStatus == Status.success)
        currentStep += 1
      await(closeOperation(operationTrace, targetStatus))
      operationTrace
    }

    def revert(defaultVersion: String = ""): DeepOperationTrace = {
      await(
        for {
          operationTrace <- crankshaft.revert(deploymentRequest, Some(currentState.next()), "r.everter", defaultVersion.headOption.map(_ => Version(defaultVersion.toJson)))
          _ <- closeOperation(operationTrace)
        } yield operationTrace
      )
    }
  }

}
