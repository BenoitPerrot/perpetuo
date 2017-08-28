package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.dao.{ProductCreationConflict, Schema, UnknownProduct}
import com.criteo.perpetuo.engine.{Action, Engine}
import com.criteo.perpetuo.model._
import com.twitter.finagle.http.{Request, Response, Status => HttpStatus}
import com.twitter.finatra.http.exceptions.{BadRequestException, ConflictException, HttpException}
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import com.twitter.util.{Future => TwitterFuture}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException
import spray.json.{DeserializationException, _}

import scala.collection.SortedMap
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
                                        offset: Int = 0,
                                        @Inject request: Request)

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
      5.seconds
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
        5.seconds
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
      engine.findDeepDeploymentRequestById(_).map(_.map(_.toJsonReadyMap)),
      5.seconds
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
        5.seconds // fixme: get back to 2 seconds when the listener will be called asynchronously
      )
    }
  }

  // TODO: migrate clients then remove <<
  put("/api/deployment-requests/:id") { r: RequestWithId =>
    authenticate(r.request) { case user if isAuthorized(user) =>
      withIdAndRequest(
        (id, _: RequestWithId) => engine.startDeploymentRequest(id, user.name).map(_.map { _ => response.ok.json(Map("id" -> id)) }),
        2.seconds
      )(r)
    }
  }
  // >>

  post("/api/deployment-requests/:id/actions") { r: RequestWithId =>
    authenticate(r.request) { case user if isAuthorized(user) =>
      withIdAndRequest(
        (id, _: RequestWithId) => {
          val actionName = try {
            r.request.contentString.parseJson.asJsObject.fields("name").asInstanceOf[JsString].value
          } catch {
            case e@(_: ParsingException | _: DeserializationException | _: NoSuchElementException | _: ClassCastException) =>
              throw BadRequestException(e.getMessage)
          }
          val action = try {
            Action.withName(actionName)
          } catch {
            case _: NoSuchElementException => throw BadRequestException(s"Action $actionName doesn't exist")
          }
          val effect = action match {
            case Action.applyFirst => engine.startDeploymentRequest _
            case Action.applyAgain => engine.deployAgain _
            case Action.rollback => engine.rollbackDeploymentRequest _
          }

          engine.isStarted(id)
            .flatMap { started =>
              if (!engine.getActionsIf(started).contains(action))
                throw new IllegalStateException(if (started) "it has been started already" else "it hasn't been applied yet")
              engine.getConflictReason(id, started).map(_.map(reason => throw new IllegalStateException(reason)))
            }
            .flatMap(_ => effect(id, user.name))
            .map(_.map(_ => response.ok.json(Map("id" -> id))))
            .recover { case e: IllegalStateException =>
              throw BadRequestException(s"Cannot $actionName the deployment request #$id: ${e.getMessage}")
            }
        },
        5.seconds
      )(r)
    }
  }

  get("/api/deployment-requests/:id/execution-traces")(
    withLongId(
      engine.findExecutionTracesByDeploymentRequest,
      5.seconds
    )
  )
  // TODO: migrate clients to "/api/deployment-requests/:id/execution-traces" then remove <<
  get("/api/execution-traces/by-deployment-request/:id")(
    withLongId(
      engine.findExecutionTracesByDeploymentRequest,
      5.seconds
    )
  )
  // >>

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

  get("/api/deployment-requests/:id/operation-traces")(
    withLongId(
      engine.findOperationTracesByDeploymentRequest,
      5.seconds
    )
  )
  // TODO: migrate clients to "/api/deployment-requests/:id/operation-traces" then remove <<
  get("/api/operation-traces/by-deployment-request/:id")(
    withLongId(
      engine.findOperationTracesByDeploymentRequest,
      5.seconds
    )
  )
  // >>

  private def computeState(sortedGroupsOfExecutions: SortedMap[Long, Seq[ExecutionTrace]]): String =
    if (sortedGroupsOfExecutions.isEmpty || sortedGroupsOfExecutions.last._2.isEmpty) {
      "not-started"
    } else {
      val lastOperationTrace = sortedGroupsOfExecutions.last._2.head.operationTrace
      val lastExecutionTraces = sortedGroupsOfExecutions.last._2

      val operationIsFinished = lastOperationTrace.closingDate.nonEmpty
      val operationHasFailures =
        lastExecutionTraces.exists(_.state == ExecutionState.initFailed) || lastOperationTrace.targetStatus.values.exists(_.code != Status.success)

      val lastOperationState = if (operationIsFinished) {
        if (operationHasFailures) {
          "failed"
        } else {
          "succeeded"
        }
      } else {
        "in-progress"
      }

      s"${lastOperationTrace.kind.toString} $lastOperationState"
    }

  private def serialize(isAuthorized: Boolean, depReq: DeepDeploymentRequest, sortedGroupsOfExecutions: SortedMap[Long, Seq[ExecutionTrace]]): Map[String, Any] =
    Map(
      "id" -> depReq.id,
      "comment" -> depReq.comment,
      "creationDate" -> depReq.creationDate,
      "creator" -> depReq.creator,
      "version" -> depReq.version,
      "target" -> RawJson(depReq.target),
      "productName" -> depReq.product.name,
      "state" -> computeState(sortedGroupsOfExecutions)
    )

  private def serialize(executionResultsGroups: SortedMap[Long, (Iterable[ExecutionTrace], Iterable[TargetStatus])]): Iterable[Map[String, Any]] =
    executionResultsGroups.map { case (_, (executionTraces, targetStatus)) =>
      val op = executionTraces.head.operationTrace
      Map(
        "id" -> op.id,
        "kind" -> op.kind.toString,
        "creator" -> op.creator,
        "creationDate" -> op.creationDate,
        "targetStatus" -> {
          val x = targetStatus.map(targetAtomStatus =>
            targetAtomStatus.targetAtom -> Map("code" -> targetAtomStatus.code.toString, "detail" -> targetAtomStatus.detail))
          Map[String, Map[String, String]](x.toSeq: _*)
        },
        "executions" -> executionTraces
      ) ++ op.closingDate.map("closingDate" -> _)
    }

  get("/api/unstable/deployment-requests/:id") { r: RequestWithId =>
    withLongId(id =>
      engine.findDeepDeploymentRequestAndExecutions(id)
        .map(_.map { case (deploymentRequest, executionResultGroups) =>
          val authorized = r.request.user.exists(isAuthorized)
          val sortedGroupsOfExecutions = executionResultGroups.mapValues(_._1.toSeq)
          val actions = engine.getActionsIf(sortedGroupsOfExecutions.nonEmpty).map(action =>
            Map("name" -> action.toString, "authorized" -> authorized)
          ) // todo: future workflow will provide different actions for different permissions

          serialize(authorized, deploymentRequest, sortedGroupsOfExecutions) ++
            Map("operations" -> serialize(executionResultGroups)) ++
            Map("actions" -> actions)
        }),
      5.seconds
    )(r)
  }
  post("/api/unstable/deployment-requests") { r: SortingFilteringPost =>
    timeBoxed(
      {
        if (1000 < r.limit) {
          throw BadRequestException("`limit` shall be lower than 1000")
        }
        try {
          engine.queryDeepDeploymentRequests(r.where, r.orderBy, r.limit, r.offset)
            .map(_.map { case (deploymentRequest, executionTraces) =>
              serialize(r.request.user.exists(isAuthorized), deploymentRequest, executionTraces)
            })
        } catch {
          case e: IllegalArgumentException => throw BadRequestException(e.getMessage)
        }
      },
      5.seconds)
  }

  // todo: remove, it's for migration only
  post("/api/unstable/db/operation-traces/set-missing-closing-date") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.setOperationTracesMissingClosingDate().map(x => Map("status" -> x)), 2.hours)
  }
  get("/api/unstable/db/operation-traces/missing-closing-date-count") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.countOperationTracesMissingClosingDate().map(x => Map("count" -> x)), 2.seconds)
  }


  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    response.notFound
  }

}


object RestApi {
  def executionCallbackPath(execTraceId: String): String = s"/api/execution-traces/$execTraceId"
}
