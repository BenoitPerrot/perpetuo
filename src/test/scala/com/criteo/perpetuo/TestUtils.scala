package com.criteo.perpetuo

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CompletableFuture, Future => JavaFuture}

import com.criteo.perpetuo.app.RestApi
import com.criteo.perpetuo.auth.{Permissions, Unrestricted, User}
import com.criteo.perpetuo.config.{AppConfig, TestConfig}
import com.criteo.perpetuo.dao.{DbBinding, DbContext, DbContextProvider, TestingDbContextModule}
import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.executors.{ExecutionTrigger, TriggeredExecutionFinder}
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model._
import com.google.common.base.Ticker
import com.google.inject._
import com.twitter.inject.{Test, TwitterModule}
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
  lazy val dbTestModule = new TestingDbContextModule(TestConfig.config.getConfig("db"))
  lazy val dbContext: DbContext = dbTestModule.providesDbContext
  lazy val dbBinding = new DbBinding(dbContext)
}

class MockTicker(jumpSize: Duration) extends Ticker {
  private val nanos = new AtomicLong

  override def read: Long = nanos.get

  def jump(): Unit = nanos.addAndGet(jumpSize.toNanos)
}

trait TestHelpers extends Test {
  def asynchronouslyThrow[T <: Throwable : ClassTag](pattern: String): Matcher[Future[_]] =
    be(a[T]).and(fullyMatch.regex(pattern).compose((_: Throwable).getMessage)).compose(f => await(f.failed))

  def become[T](value: T): Matcher[Future[T]] = eventually(be(value))

  def eventually[T](matcher: Matcher[T]): Matcher[Future[T]] = matcher.compose(await)

  def await[T](a: Awaitable[T]): T = Await.result(a, 2.seconds)
}


object TestTargetResolver extends TargetResolver {
  protected override def resolveNonAtoms(productName: String, productVersion: Version, targetTerms: Set[TargetNonAtom]): (Map[TargetNonAtom, Set[TargetAtom]], Boolean) = {
    // the atomic targets are the input tags split on dashes
    (targetTerms.map {
      case t@TargetTag(tag) => t -> tag.split("-").collect { case name if name.nonEmpty => TargetAtom(name) }.toSet
      case t: TargetNonAtom => t -> Set.empty[TargetAtom]
    }.toMap, true)
  }
}


trait SimpleScenarioTesting extends TestHelpers with TestDb with MockitoSugar {

  import dbContext.profile.api._

  private val knownProducts = mutable.Set[String]()

  protected def providesAppConfig: AppConfig = TestConfig

  protected def providesPermissions: Permissions = Unrestricted

  protected def providesTargetDispatcher: TargetDispatcher = new SingleTargetDispatcher(executionTrigger)

  protected def providesTargetResolver: TargetResolver = new TargetResolver {}

  protected def providesPreConditionEvaluators: Seq[AsyncPreConditionEvaluator] = Seq()

  protected def providesListeners: Seq[AsyncListener] = Seq()

  protected def extraModules: Seq[Module] = Seq[Module]()

  protected val injector: Injector = Guice.createInjector(
    extraModules.:+(
      new TwitterModule() {
        @Singleton
        @Provides
        def providesDbContext: DbContext = dbContext

        @Singleton
        @Provides
        def providesDbBinding: DbBinding = dbBinding

        @Singleton
        @Provides
        def providesAppConfig: AppConfig = SimpleScenarioTesting.this.providesAppConfig

        @Singleton
        @Provides
        def providesTargetDispatcher: TargetDispatcher = SimpleScenarioTesting.this.providesTargetDispatcher

        @Singleton
        @Provides
        def providesTargetResolver: TargetResolver = SimpleScenarioTesting.this.providesTargetResolver

        @Singleton
        @Provides
        def providesPermissions: Permissions = SimpleScenarioTesting.this.providesPermissions

        @Singleton
        @Provides
        def providesListeners: Seq[AsyncListener] = SimpleScenarioTesting.this.providesListeners

        @Singleton
        @Provides
        def providesPreConditionEvaluators: Seq[AsyncPreConditionEvaluator] = SimpleScenarioTesting.this.providesPreConditionEvaluators

        @Singleton
        @Provides
        def providesCrankshaft(appConfig: AppConfig, fuelFilter: FuelFilter, listeners: Seq[AsyncListener], executionFinder: TriggeredExecutionFinder, targetDispatcher: TargetDispatcher, restApi: RestApi): Crankshaft = {
          when(executionTrigger.trigger(restApi.executionCallbackUrl(anyLong), anyString, any[Version], any[TargetAtomSet], anyString))
            .thenReturn(Future(triggerMock))
          new Crankshaft(dbBinding, fuelFilter, targetDispatcher, listeners, executionFinder, restApi)
        }

        @Singleton
        @Provides
        def providesEngine(appConfig: AppConfig, crankshaft: Crankshaft, fuelFilter: FuelFilter, resolver: TargetResolver, permissions: Permissions, preConditionEvaluators: Seq[AsyncPreConditionEvaluator]): Engine = {
          class NoOpScheduler extends Scheduler {
            override def scheduleTask(f: () => Any, period: Long, timeUnit: TimeUnit, initialDelay: Long): JavaFuture[_] =
              CompletableFuture.completedFuture(())
          }

          new Engine(appConfig, crankshaft, fuelFilter, resolver, permissions, preConditionEvaluators, new NoOpScheduler()) {
            override val stateExpirationTime: FiniteDuration = 1000.seconds // the goal is that no test actually depends on it
            override val ticker: Ticker = mockTicker // ... thanks to this mock
          }
        }
      }
    ): _*
  )

  private val executionTrigger: ExecutionTrigger = mock[ExecutionTrigger]
  when(executionTrigger.executorType).thenReturn("testing")

  protected val mockTicker = new MockTicker(1001.seconds)

  protected lazy val crankshaft: Crankshaft = injector.getInstance(classOf[Crankshaft])
  protected lazy val engine: Engine = injector.getInstance(classOf[Engine])

  protected def triggerMock: Option[String] = None

  val requesterName = "r.equester"
  val officerName = "o.fficer"

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

  def tryStopOperation(operationTrace: OperationTrace, initiatorName: String): Future[(Int, Seq[String])] =
    crankshaft.findDeploymentEffects(operationTrace.deploymentRequest.id).flatMap(effects =>
      crankshaft.tryStopOperation(effects.get.maxBy(_.operationTrace.id), initiatorName)
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
                crankshaft.updateExecutionTrace(_, executionState, "", None, finalStatusMap).map { case (opTrace, _) => Some(opTrace.id) }
                  .recover { case _: UnavailableAction => None }
              )
            }
        )
      )
      .map(_.flatten)

  def tryUpdateExecutionTraces(engine: Engine, executionTraces: Iterable[ShallowExecutionTrace],
                               state: ExecutionState.Value, detail: String = "", href: Option[String] = None,
                               statusMap: Map[TargetAtom, TargetAtomStatus] = Map()): Future[Iterable[Option[(OperationTrace, Boolean)]]] =
    Future.sequence(
      executionTraces.map(executionTrace => engine.tryUpdateExecutionTrace(executionTrace.id, state, detail, href, statusMap))
    )

  def request(productName: String, version: String, stepsTargets: Iterable[String]*): RequestTesting = {
    if (!knownProducts.contains(productName)) {
      await(crankshaft.upsertProduct(productName))
      knownProducts += productName
    }

    new RequestTesting(productName, version, stepsTargets)
  }

  class RequestTesting(productName: String, version: String, stepsTargets: Seq[Iterable[String]]) {
    val deploymentPlan: DeploymentPlan = {
      val targetExpressions: Seq[JsValue] = stepsTargets.map(_
        .map {
          case s if s.startsWith("tag:") => JsObject("tag" -> JsString(s.substring(4, s.length))): JsValue
          case s => JsString(s): JsValue
        }
        .toJson
      )
      await(crankshaft.createDeploymentPlan(ProtoDeploymentRequest(
        productName,
        Version(version.toJson),
        targetExpressions.zipWithIndex.map { case (target, i) => ProtoDeploymentPlanStep((i + 1).toString, target, "") },
        "",
        requesterName
      )))
    }
    private val currentState = Iterator.from(0)
    private var currentStep = 0

    def eligibleOperations: Future[Seq[Operation.Kind]] =
      crankshaft
        .assessDeploymentState(getDeploymentRequest)
        .map {
          case s if s.isOutdated => Seq()
          case _: Reverted => Seq()
          case _: NotStarted | _: DeployFlopped => Seq(Operation.deploy)
          case _: DeployFailed | _: Paused => Seq(Operation.deploy, Operation.revert)
          case _: Deployed | _: RevertFailed => Seq(Operation.revert)
          case _ => throw new RuntimeException("should not be there")
        }

    def startStep(): OperationTrace = {
      await(SimpleScenarioTesting.this.step(getDeploymentRequest, Some(currentState.next()), officerName))
    }

    def step(finalStatus: Status.Code = Status.success): OperationTrace = {
      val targetStatus = try {
        stepsTargets(currentStep).map(_ -> finalStatus).toMap
      } catch {
        case _: ArrayIndexOutOfBoundsException => Map[String, Status.Code]() // This shouldn't be necessary. If there isn't anymore steps, the deployment transaction is closed.
      }
      step(targetStatus)
    }

    def step(s: Map[String, Status.Code]): OperationTrace = {
      val operationTrace = startStep()
      if (s.values.forall(_ == Status.success))
        currentStep += 1
      await(closeOperation(operationTrace, s))
      operationTrace
    }

    def revert(defaultVersion: String = "", finalStatus: Status.Code = Status.success): OperationTrace = {
      val targetStatus = stepsTargets.zipWithIndex.collect { case (target, index) if index < currentStep => target.map(_ -> finalStatus) }.flatten.toMap
      await(
        for {
          operationTrace <- SimpleScenarioTesting.this.revert(getDeploymentRequest, Some(currentState.next()), officerName, defaultVersion.headOption.map(_ => Version(defaultVersion.toJson)))
          _ <- closeOperation(operationTrace, targetStatus)
        } yield operationTrace
      )
    }

    def stop(): Option[(Int, Seq[String])] =
      await(engine.stop(User(officerName), getDeploymentRequest.id, Some(currentState.next())))

    def completeExecution(operationTrace: OperationTrace, targetStatuses: Map[String, Status.Code], executor: Option[String] = Some("http://executor")): Unit =
      await(
        for {
          executionTraces <- crankshaft.dbBinding.findExecutionTracesByOperationTrace(operationTrace.id)
          _ <- tryUpdateExecutionTraces(engine, executionTraces, ExecutionState.completed, "", executor, targetStatuses.map { case (name, code) => (TargetAtom(name), TargetAtomStatus(code, "")) })
        } yield ()
      )

    def state: DeploymentRequestState.Value =
      await(crankshaft.findDeploymentRequestById(getDeploymentRequest.id)).get.state.get

    def getDeploymentRequest: DeploymentRequest =
      deploymentPlan.deploymentRequest
  }

}
