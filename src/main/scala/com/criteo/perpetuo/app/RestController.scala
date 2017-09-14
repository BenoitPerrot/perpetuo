package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.dao.{ProductCreationConflict, Schema, UnknownProduct}
import com.criteo.perpetuo.engine.{Engine, UnprocessableAction}
import com.criteo.perpetuo.model._
import com.twitter.finagle.http.{Request, Response, Status => HttpStatus}
import com.twitter.finatra.http.exceptions.{BadRequestException, ConflictException, HttpResponseException}
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
import scala.concurrent.{Await, Future}
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
                                     detail: String = "",
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
  extends BaseController with TimeoutToHttpStatusTranslation {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")
  private val deployBotName = "qabot"
  private val escalationTeamNames = List(
    "e.peroumalnaik", "g.bourguignon", "m.runtz", "m.molongo",
    "m.nguyen", "m.soltani", "s.guerrier", "t.tellier", "t.zhuang",
    "e.moutarde", "d.michau"
  )

  private def permissions = engine.plugins.permissions

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
          val (actionName, defaultVersion) = try {
            val body = r.request.contentString.parseJson.asJsObject.fields
            (body("type").asInstanceOf[JsString].value, body.get("defaultVersion").map(Version.apply))
          } catch {
            case e@(_: ParsingException | _: DeserializationException | _: NoSuchElementException | _: ClassCastException) =>
              throw BadRequestException(e.getMessage)
          }
          val action = try {
            Operation.withName(actionName)
          } catch {
            case _: NoSuchElementException => throw BadRequestException(s"Action $actionName doesn't exist")
          }

          engine.isDeploymentRequestStarted(id)
            .flatMap(
              _.map { case (deploymentRequest, isStarted) =>
                val effect = action match {
                  case Operation.deploy if isStarted => engine.deployAgain _
                  case Operation.deploy => engine.startDeploymentRequest _
                  case Operation.revert => engine.rollbackDeploymentRequest(_: Long, _: String, defaultVersion)
                }
                engine
                  .actionChecker(deploymentRequest, isStarted)(action)
                  .flatMap(_ => effect(id, user.name))
                  .recover { case e: UnprocessableAction =>
                    val body = Map("errors" -> Seq(s"Cannot $actionName: ${e.msg}")) ++
                      e.required.map("required" -> _)
                    throw new HttpResponseException(response.EnrichedResponse(HttpStatus.UnprocessableEntity).json(body))
                  }
                  .map(_.map(_ => response.ok.json(Map("id" -> id))))
              }.getOrElse(Future.successful(None))
            )
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
        engine.updateExecutionTrace(id, executionState, r.detail, r.logHref, statusMap).map(_.map(_ => response.noContent))
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

  private def computeState(sortedGroupsOfExecutions: Iterable[Iterable[ExecutionTrace]]): String =
    if (sortedGroupsOfExecutions.isEmpty) {
      "not-started"
    } else {
      val lastExecutionTraces = sortedGroupsOfExecutions.last
      val lastOperationTrace = lastExecutionTraces.head.operationTrace

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

  private def serialize(isAuthorized: Boolean, depReq: DeepDeploymentRequest, sortedGroupsOfExecutions: Iterable[Iterable[ExecutionTrace]]): Map[String, Any] =
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

  private def serialize(executionResultsGroups: Iterable[(Iterable[ExecutionTrace], Iterable[TargetStatus])]): Iterable[Map[String, Any]] =
    executionResultsGroups.map { case (executionTraces, targetStatus) =>
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
        .flatMap(_.map { case (deploymentRequest, sortedGroupsOfExecutionsAndResults) =>
          val authorized = r.request.user.exists(isAuthorized)
          val sortedGroupsOfExecutions = sortedGroupsOfExecutionsAndResults.map(_._1)
          val check = engine.actionChecker(deploymentRequest, sortedGroupsOfExecutions.nonEmpty)
          Future
            .sequence(
              Operation.values.map(action =>
                check(action)
                  .map(_ => Some(Map("type" -> action.toString, "authorized" -> authorized)))
                  .recover { case _ => None }
              ) // todo: future workflow will provide different actions for different permissions
            )
            .map(actions =>
              Some(serialize(authorized, deploymentRequest, sortedGroupsOfExecutions) ++
                Map("operations" -> serialize(sortedGroupsOfExecutionsAndResults)) ++
                Map("actions" -> actions.flatten))
            )
        }.getOrElse(Future.successful(None))),
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
  post("/api/unstable/db/operation-traces/remove-old-fks") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.removeOldFks().map(x => Map("status" -> x)), 2.hours)
  }
  get("/api/unstable/db/operation-traces/count-old-fks") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.countOldFks().map(x => Map("count" -> x)), 2.seconds)
  }
  post("/api/unstable/db/execution-traces/set-missing-details") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.setExecutionTracesMissingDetails().map(x => Map("status" -> x)), 2.hours)
  }
  get("/api/unstable/db/execution-traces/missing-details-count") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.countExecutionTracesMissingDetails().map(x => Map("count" -> x)), 2.seconds)
  }
  post("/api/unstable/db/target-statuses/remove-init-failure-details") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.removeInitFailureDetails().map(x => Map("status" -> x)), 2.hours)
  }
  get("/api/unstable/db/target-statuses/count-init-failure-details") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.countInitFailureDetails().map(x => Map("count" -> x)), 2.seconds)
  }


  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    response.notFound
  }

}


object RestApi {
  def executionCallbackPath(execTraceId: String): String = s"/api/execution-traces/$execTraceId"
}
