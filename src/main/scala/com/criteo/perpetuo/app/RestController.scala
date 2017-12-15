package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, User}
import com.criteo.perpetuo.dao.{ProductCreationConflict, Schema, UnknownProduct}
import com.criteo.perpetuo.engine.dispatchers.UnprocessableIntent
import com.criteo.perpetuo.engine.{Engine, OperationStatus, UnprocessableAction}
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
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
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

private case class ActionPost(@RouteParam @NotEmpty id: String,
                              @RouteParam @NotEmpty actionType: String,
                              @NotEmpty defaultVersion: Option[String] = None,
                              @Inject request: Request) extends WithId

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

  private def permissions = engine.permissions

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

  get("/api/products") { _: Request =>
    timeBoxed(
      engine.getProductNames,
      5.seconds
    )
  }

  post("/api/products") { r: ProductPost =>
    authenticate(r.request) { case user if permissions.isAuthorized(user.name, GeneralAction.addProduct) =>
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
    authenticate(r) { case user =>
      val (allAttrs, targets) = try {
        val attrs = DeploymentRequestParser.parse(r.contentString, user.name)
        (attrs, attrs.parsedTarget.select)
      } catch {
        case e: ParsingException => throw BadRequestException(e.getMessage)
      }

      def authorized(actionType: DeploymentAction.Value) =
        permissions.isAuthorized(user.name, actionType, Operation.deploy, allAttrs.productName, targets)

      if (!authorized(DeploymentAction.requestOperation) || (autoStart && !authorized(DeploymentAction.applyOperation)))
        throw new HttpResponseException(response.forbidden)

      timeBoxed(
        {
          val resp = try {
            engine.createDeploymentRequest(allAttrs, autoStart)
          } catch {
            case e: UnprocessableIntent => throw BadRequestException(e.getMessage)
          }
          resp.map(x => Some(response.created.json(x)))
            .recover {
              case e: UnknownProduct => throw BadRequestException(s"Product `${e.productName}` could not be found")
            }
        },
        5.seconds // fixme: get back to 2 seconds when the listener will be called asynchronously
      )
    }
  }

  // TODO: migrate clients then remove <<
  put("/api/deployment-requests/:id") { r: RequestWithId =>
    authenticate(r.request) { case user if user.name == "qabot" =>
      withIdAndRequest(
        (id, _: RequestWithId) => engine.startDeploymentRequest(id, user.name).map(_.map { _ => response.ok.json(Map("id" -> id)) }),
        2.seconds
      )(r)
    }
  }
  // >>

  post("/api/deployment-requests/:id/actions/devise-revert-plan") { r: RequestWithId =>
    withIdAndRequest(
      (id, _: RequestWithId) => {
        engine.isDeploymentRequestStarted(id).flatMap(
          _.map { case (deploymentRequest, isStarted) =>
            engine.canRevertDeploymentRequest(deploymentRequest, isStarted)
              .recover { case e: UnprocessableAction =>
                val body = Map("errors" -> Seq(s"Cannot revert the request #${deploymentRequest.id}: ${e.msg}")) ++ e.detail
                throw new HttpResponseException(response.EnrichedResponse(HttpStatus.UnprocessableEntity).json(body))
              }
              .flatMap { _ =>
                engine.findExecutionSpecificationsForRollback(deploymentRequest).map { case (undetermined, determined) =>
                  Some(Map(
                    "undetermined" -> undetermined,
                    "determined" -> determined.toStream.map { case (execSpec, targets) =>
                      Map("version" -> execSpec.version, "targetAtoms" -> targets)
                    }
                  ))
                }
              }
          }.getOrElse(Future.successful(None))
        )
      },
      5.seconds
    )(r)
  }

  post("/api/deployment-requests/:id/actions/:actionType") { r: ActionPost =>
    authenticate(r.request) { case user =>
      withIdAndRequest(
        (id, _: ActionPost) => {
          engine.isDeploymentRequestStarted(id)
            .flatMap(
              _.map { case (deploymentRequest, isStarted) =>
                val operation = try {
                  Operation.withName(r.actionType)
                } catch {
                  case _: NoSuchElementException => throw BadRequestException(s"Action ${r.actionType} doesn't exist")
                }
                val targets = deploymentRequest.parsedTarget.select
                if (permissions.isAuthorized(user.name, DeploymentAction.applyOperation, operation, deploymentRequest.product.name, targets)) {
                  val (checking, effect) = operation match {
                    case Operation.deploy => (engine.canDeployDeploymentRequest(deploymentRequest), if (isStarted) engine.deployAgain _ else engine.startDeploymentRequest _)
                    case Operation.revert => (engine.canRevertDeploymentRequest(deploymentRequest, isStarted), engine.rollbackDeploymentRequest(_: Long, _: String, r.defaultVersion.map(Version.apply)))
                  }
                  checking
                    .flatMap(_ => effect(id, user.name))
                    .recover { case e: UnprocessableAction =>
                      val body = Map("errors" -> Seq(s"Cannot ${r.actionType} the request #$id: ${e.msg}")) ++ e.detail
                      throw new HttpResponseException(response.EnrichedResponse(HttpStatus.UnprocessableEntity).json(body))
                    }
                    .map(_.map(_ => response.ok.json(Map("id" -> id))))
                }
                else
                  Future.successful(Some(response.forbidden))
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

  private def serialize(depReq: DeepDeploymentRequest, lastOperationEffect: Option[OperationEffect]): Map[String, Any] =
    Map(
      "id" -> depReq.id,
      "comment" -> depReq.comment,
      "creationDate" -> depReq.creationDate,
      "creator" -> depReq.creator,
      "version" -> depReq.version,
      "target" -> RawJson(depReq.target),
      "productName" -> depReq.product.name,
      "state" -> lastOperationEffect // for the UI: below is what will be displayed (and it must match css classes)
        .map(engine.computeState)
        .map {
          case (Operation.deploy, OperationStatus.succeeded) => "deployed"
          case (Operation.revert, OperationStatus.succeeded) => "reverted"
          case (op, state) => s"$op $state"
        }
        .getOrElse("notStarted")
    )

  private def serialize(sortedEffects: Iterable[OperationEffect]): Iterable[Map[String, Any]] =
    sortedEffects.map { case OperationEffect(op, executionTraces, targetStatus) =>
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
    withLongId(
      engine
        .findDeepDeploymentRequestAndEffects(_)
        .flatMap(_.map { case (deploymentRequest, sortedEffects) =>
          val targets = deploymentRequest.parsedTarget.select
          val isAdmin = r.request.user.exists(user =>
            permissions.isAuthorized(user.name, GeneralAction.administrate)
          )

          // todo: a future workflow will differentiate requests and applies
          def authorized(op: Operation.Kind) = r.request.user.exists(user =>
            permissions.isAuthorized(user.name, DeploymentAction.applyOperation, op, deploymentRequest.product.name, targets)
          )

          Future
            .sequence(
              Operation.values.map(action =>
                (action match {
                  case Operation.deploy => engine.canDeployDeploymentRequest(deploymentRequest)
                  case Operation.revert => engine.canRevertDeploymentRequest(deploymentRequest, sortedEffects.nonEmpty)
                })
                  .map(_ => Some(Map("type" -> action.toString, "authorized" -> authorized(action))))
                  .recover { case _ => None }
              )
            )
            .map(actions =>
              Some(serialize(deploymentRequest, sortedEffects.lastOption) ++
                Map("operations" -> serialize(sortedEffects)) ++
                Map("actions" -> actions.flatten) ++
                Map("showExecutionLogs" -> (isAdmin || authorized(Operation.deploy)))) // fixme: only as long as we can't show the logs to anyone
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
          // todo: uniform with other deep request
          engine.queryDeepDeploymentRequests(r.where, r.orderBy, r.limit, r.offset)
            .map(_.map { case (deploymentRequest, lastOperationEffect) =>
              serialize(deploymentRequest, lastOperationEffect)
            })
        } catch {
          case e: IllegalArgumentException => throw BadRequestException(e.getMessage)
        }
      },
      5.seconds)
  }

  post("/api/unstable/db/execution-traces/set-detail-from-star-statuses") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.setExecutionTracesDetailFromStarStatus().map(x => Map("updated" -> x)), 2.hours)
  }
  get("/api/unstable/db/execution-traces/count-missing-details") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.countMissingExecutionTraceDetails().map(x => Map("count" -> x)), 10.seconds)
  }
  post("/api/unstable/db/target-statuses/remove-star-statuses") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.removeRemainingStarStatuses().map(x => Map("removed" -> x)), 2.hours)
  }
  get("/api/unstable/db/target-statuses/count-star-statuses") { _: Request =>
    val schema = new Schema(engine.dbBinding.dbContext)
    Await.result(schema.countStarStatuses().map(x => Map("count" -> x)), 10.seconds)
  }

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    response.notFound
  }

}


object RestApi {
  def executionCallbackPath(execTraceId: String): String = s"/api/execution-traces/$execTraceId"
}
