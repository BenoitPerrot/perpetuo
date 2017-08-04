package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.dao.{ProductCreationConflict, Schema, UnknownProduct}
import com.criteo.perpetuo.engine.Engine
import com.criteo.perpetuo.model._
import com.twitter.finagle.http.{Request, Response, Status => HttpStatus}
import com.twitter.finatra.http.exceptions.{BadRequestException, ConflictException, HttpException}
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import com.twitter.util.{Future => TwitterFuture}
import spray.json.DefaultJsonProtocol._
import spray.json.DeserializationException
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.Try

trait WithId {
  val id: String
}

@JsonIgnoreBody
private case class RequestWithId(@RouteParam @NotEmpty id: String,
                                 @Inject request: Request) extends WithId

private case class ProductPost(@NotEmpty name: String,
                               @Inject request: Request)

private case class ProductPostWithVersion(@NotEmpty name: String,
                                          @NotEmpty version: String)

private case class ExecutionTracePut(@RouteParam @NotEmpty id: String,
                                     @NotEmpty state: String,
                                     logHref: String = "",
                                     targetStatus: Map[String, Map[String, String]] = Map(),
                                     @Inject request: Request) extends WithId

private case class SortingFilteringPost(orderBy: Seq[Map[String, Any]] = Seq(),
                                        where: Seq[Map[String, Any]] = Seq(),
                                        limit: Int = 20,
                                        offset: Int = 0)

/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val engine: Engine)
  extends BaseController {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")
  private val deployBotName = "qabot"
  private val escalationTeamNames = List(
    "e.peroumalnaik", "g.bourguignon", "m.runtz", "m.molongo",
    "m.nguyen", "m.soltani", "s.guerrier", "t.tellier", "t.zhuang",
    "e.moutarde", "d.michau"
  )

  private def handleTimeout[T](action: => T): T =
    try {
      action
    }
    catch {
      case e: TimeoutException => throw HttpException(HttpStatus.GatewayTimeout, e.getMessage)
    }

  private def await[T](future: Future[T], maxDuration: Duration): T =
    handleTimeout(Await.result(future, maxDuration))

  private def timeBoxed[T](view: => Future[T], maxDuration: Duration): TwitterFuture[T] =
    futurePool {
      await(view, maxDuration)
    }

  private def withLongId[T](view: Long => Future[Option[T]], maxDuration: Duration): RequestWithId => TwitterFuture[Option[T]] =
    withIdAndRequest[RequestWithId, T]({ case (id, _) => view(id) }, maxDuration)

  private def withIdAndRequest[I <: WithId, O](view: (Long, I) => Future[Option[O]], maxDuration: Duration): I => TwitterFuture[Option[O]] =
    request => futurePool {
      Try(request.id.toLong).toOption.map(view(_, request)).flatMap(await(_, maxDuration))
    }

  private def authenticate(r: Request)(callback: PartialFunction[User, TwitterFuture[Option[Response]]]): TwitterFuture[Option[Response]] = {
    r.user
      .map(callback.orElse { case _ => TwitterFuture(Some(response.forbidden)) })
      .getOrElse(TwitterFuture(Some(response.unauthorized)))
  }

  // todo: Use AD group to give escalation team permissions to deploy in prod
  private def isAuthorized(user: User): Boolean =
    user.name == deployBotName || escalationTeamNames.contains(user.name) || AppConfig.env != "prod"

  get("/api/products") { _: Request =>
    timeBoxed(
      engine.getProductNames,
      2.seconds
    )
  }

  post("/api/products") { r: ProductPost =>
    authenticate(r.request) { case user if user.name == deployBotName =>
      timeBoxed(
        engine.insertProduct(r.name)
          .map(_ => Some(response.created.nothing))
          .recover { case e: ProductCreationConflict =>
            throw ConflictException(e.getMessage)
          },
        2.seconds
      )
    }
  }

  post("/api/products/suggest-versions") { r: ProductPost =>
    handleTimeout(
      engine.suggestVersions(r.name)
    )
  }

  post("/api/products/validate-version") { r: ProductPostWithVersion =>
    handleTimeout {
      val reasonsForInvalidity = engine.validateVersion(r.name, r.version)
      if (reasonsForInvalidity.isEmpty)
        Map("valid" -> true)
      else
        Map("valid" -> false, "reason" -> reasonsForInvalidity)
    }
  }

  get("/api/deployment-requests/:id")(
    withLongId(
      engine.findDeploymentRequestByIdWithProduct(_).map(_.map(_.toJsonReadyMap)),
      2.seconds
    )
  )

  post("/api/deployment-requests") { r: Request =>
    val autoStart = r.getBooleanParam("start", default = false)
    authenticate(r) { case user if user.name == deployBotName || !autoStart =>
      timeBoxed(
        try {
          engine.createDeploymentRequest(DeploymentRequestParser.parse(r.contentString, user.name), autoStart)
            .map(x => Some(response.created.json(x)))
            .recover {
              case e: UnknownProduct => throw BadRequestException(s"Product `${e.productName}` could not be found")
            }
        } catch {
          case e: ParsingException => throw BadRequestException(e.getMessage)
        },
        5.seconds // fixme: get back to 2 seconds when the hook will be called asynchronously
      )
    }
  }

  put("/api/deployment-requests/:id") { r: RequestWithId =>
    authenticate(r.request) { case user if isAuthorized(user) =>
      withIdAndRequest(
        (id, _: RequestWithId) => engine.startDeploymentRequest(id, user.name).map(_.map { _ => response.ok.json(Map("id" -> id)) }),
        2.seconds
      )(r)
    }
  }

  get("/api/execution-traces/by-deployment-request/:id")(
    withLongId(
      engine.findExecutionTracesByDeploymentRequest,
      2.seconds
    )
  )

  put(RestApi.executionCallbackPath(":id")) {
    // todo: give the permission to Rundeck only
    withIdAndRequest(
      (id, r: ExecutionTracePut) => {
        val executionState =
          try {
            ExecutionState.withName(r.state)
          } catch {
            case _: NoSuchElementException => throw BadRequestException(s"Unknown state `${r.state}`")
          }
        val statusMap: Map[String, TargetAtomStatus] =
          try {
            r.targetStatus.map { // don't use mapValues, as it gives a view (lazy generation, incompatible with error management here)
              case (k, obj) => (k, Status.targetMapJsonFormat.read(obj.toJson)) // yes it's crazy to use spray's case class deserializer
            }
          } catch {
            case e: DeserializationException => throw BadRequestException(e.getMessage)
          }
        engine.updateExecutionTrace(id, executionState, r.logHref, statusMap).map(_.map(_ => response.noContent))
      },
      3.seconds
    )
  }

  get("/api/operation-traces/by-deployment-request/:id")(
    withLongId(
      engine.findOperationTracesByDeploymentRequest,
      2.seconds
    )
  )

  put("/api/operation-traces/:id/retry") { r: RequestWithId =>
    authenticate(r.request) { case user if isAuthorized(user) =>
      withIdAndRequest(
        (id, _: RequestWithId) => engine.retryOperationTrace(id, user.name).map(_.map { _ => response.ok.json(Map("id" -> id)) }),
        2.seconds
      )(r)
    }
  }

  private def serialize(depReq: DeploymentRequest, sortedGroupsOfExecutions: Iterable[ArrayBuffer[ExecutionTrace]]) = {
    val state =
      if (sortedGroupsOfExecutions.isEmpty) {
        "not-started"
      } else {
        val x = sortedGroupsOfExecutions.foldLeft((true, false)) { case ((isFinishedSoFar, hasFailuresSoFar), executionTraces) =>
          val operationTrace = executionTraces.head.operationTrace
          val operationIsFinished = executionTraces.forall(executionTrace =>
            executionTrace.state != ExecutionState.pending && executionTrace.state != ExecutionState.running
          )
          val operationHasFailures = executionTraces.exists(_.state == ExecutionState.initFailed) ||
            operationTrace.targetStatus.values.exists(_.code != Status.success)
          (isFinishedSoFar && operationIsFinished,
            hasFailuresSoFar || operationHasFailures)
        }
        val isFinished = x._1
        val hasFailures = x._2
        if (isFinished) {
          if (hasFailures) {
            "failed"
          } else {
            "succeeded"
          }
        } else {
          "in-progress"
        }
      }

    Map(
      "id" -> depReq.id,
      "comment" -> depReq.comment,
      "creationDate" -> depReq.creationDate,
      "creator" -> depReq.creator,
      "version" -> depReq.version,
      "target" -> RawJson(depReq.target),
      "productName" -> depReq.product.name,
      "state" -> state,
      "operations" -> sortedGroupsOfExecutions.map { execs =>
        val op = execs.head.operationTrace
        Map(
          "id" -> op.id,
          "type" -> op.operation.toString,
          "creator" -> op.creator,
          "creationDate" -> op.creationDate,
          "targetStatus" -> op.targetStatus,
          "executions" -> execs
        ) ++ op.closingDate.map("closingDate" -> _)
      }
    )
  }

  get("/api/unstable/deployment-requests") { _: Request =>
    timeBoxed(
      engine.queryDeepDeploymentRequests(where = Seq(), orderBy = Seq(), limit = 20, offset = 0)
        .map(_.map { case (deploymentRequest, executionTraces) => serialize(deploymentRequest, executionTraces) }),
      5.seconds
    )
  }
  get("/api/unstable/deployment-requests/:id")(
    withLongId(id =>
      engine.getDeepDeploymentRequest(id)
        .map(_.map { case (deploymentRequest, executionTraces) => serialize(deploymentRequest, executionTraces) }),
      2.seconds
    )
  )
  post("/api/unstable/deployment-requests") { r: SortingFilteringPost =>
    timeBoxed(
      {
        if (1000 < r.limit) {
          throw BadRequestException("`limit` shall be lower than 1000")
        }
        try {
          engine.queryDeepDeploymentRequests(r.where, r.orderBy, r.limit, r.offset)
            .map(_.map { case (deploymentRequest, executionTraces) => serialize(deploymentRequest, executionTraces) })
        } catch {
          case e: IllegalArgumentException => throw BadRequestException(e.getMessage)
        }
      },
      5.seconds)
  }

  // <<
  // TODO: remove once DB migration done
  val schema = new Schema(engine.dbBinding.dbContext)

  post("/api/unstable/db/operation-traces/set-missing-creation-date") { _: Request =>
    Await.result(schema.setOperationTracesMissingCreationDate().map(x => Map("status" -> x)), 2.hours)
  }
  get("/api/unstable/db/operation-traces/missing-creation-date-count") { _: Request =>
    Await.result(schema.countOperationTracesMissingCreationDate().map(x => Map("count" -> x)), 2.seconds)
  }

  post("/api/unstable/db/operation-traces/set-missing-closing-date") { _: Request =>
    Await.result(schema.setOperationTracesMissingClosingDate().map(x => Map("status" -> x)), 2.hours)
  }
  get("/api/unstable/db/operation-traces/missing-closing-date-count") { _: Request =>
    Await.result(schema.countOperationTracesMissingClosingDate().map(x => Map("count" -> x)), 2.seconds)
  }

  post("/api/unstable/db/executions/create-all") { r: Request =>
    Await.result(schema.createAllExecutions(r.contentString).map(x => Map("not done" -> x)), 2.hours)
  }

  get("/api/unstable/db/executions/missing-count") { _: Request =>
    Await.result(schema.countMissingExecutions().map(x => Map("count" -> x)), 2.seconds)
  }

  post("/api/unstable/db/target-status/set-missing") { _: Request =>
    Await.result(schema.setMissingTargetStatuses().map(x => Map("count" -> x)), 2.hours)
  }

  get("/api/unstable/db/target-status/count-executions-not-set") { _: Request =>
    Await.result(schema.countMissingTargetStatuses().map(x => Map("count" -> x)), 20.seconds)
  }
  // >>

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    response.notFound
  }

}


object RestApi {
  def executionCallbackPath(executionId: String): String = s"/api/execution-traces/$executionId"
}
