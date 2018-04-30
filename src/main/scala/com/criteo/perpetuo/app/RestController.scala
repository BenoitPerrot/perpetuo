package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, User}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.engine.{Crankshaft, Engine, OperationStatus, RejectingError}
import com.criteo.perpetuo.model._
import com.twitter.finagle.http.{Request, Status => HttpStatus}
import com.twitter.finatra.http.exceptions._
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import com.twitter.util.{Future => TwitterFuture}
import javax.inject.Inject
import spray.json.JsonParser.ParsingException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

trait WithId {
  val id: String
}

@JsonIgnoreBody
private case class RequestWithId(@RouteParam @NotEmpty id: String,
                                 @Inject request: Request) extends WithId

private case class RequestWithName(@NotEmpty name: String,
                                   @Inject request: Request)

private case class ProductPostWithVersion(@NotEmpty name: String,
                                          @NotEmpty version: String)

private case class RequestWithIdAndDefaultVersion(@RouteParam @NotEmpty id: String,
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
  extends BaseController with ExceptionsToHttpStatusTranslation {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")

  private val crankshaft = engine.crankshaft

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

  private def authenticate[T](r: Request)(callback: PartialFunction[User, TwitterFuture[Option[T]]]): TwitterFuture[Option[T]] = {
    r.user
      .map(callback.orElse { case _ => throw ForbiddenException() })
      .getOrElse(throw HttpException(HttpStatus.Unauthorized))
  }

  get("/api/products") { _: Request =>
    timeBoxed(
      crankshaft.getProductNames,
      5.seconds
    )
  }

  // todo: remove
  post("/api/products") { r: RequestWithName =>
    authenticate(r.request) { case user if permissions.isAuthorized(user, GeneralAction.addProduct) =>
      timeBoxed(
        crankshaft
          .insertProduct(r.name)
          .map(_ => Some(response.created.nothing)),
        5.seconds
      )
    }
  }

  put("/api/products") { r: RequestWithName =>
    authenticate(r.request) { case user if permissions.isAuthorized(user, GeneralAction.addProduct) =>
      timeBoxed(
        crankshaft
          .insertProductIfNotExists(r.name)
          .map(_ => Some(response.created.nothing)),
        5.seconds
      )
    }
  }

  get("/api/deployment-requests/:id")(
    withLongId(
      crankshaft.findDeepDeploymentRequestById(_).map(_.map(_.toJsonReadyMap)),
      5.seconds
    )
  )

  post("/api/deployment-requests") { r: Request =>
    authenticate(r) { case user =>
      val (allAttrs, targets) = try {
        val attrs = DeploymentRequestParser.parse(r.contentString, user.name)
        (attrs, attrs.parsedTarget.select)
      } catch {
        case e: ParsingException => throw BadRequestException(e.getMessage)
      }

      timeBoxed(
        engine.requestDeployment(user, allAttrs, targets)
          .map(x => Some(response.created.json(Map("id" -> x)))),
        5.seconds
      )
    }
  }

  post("/api/deployment-requests/:id/actions/devise-revert-plan") { r: RequestWithId =>
    withIdAndRequest(
      (id, _: RequestWithId) => {
        crankshaft.isDeploymentRequestStarted(id).flatMap(
          _.map { case (deploymentRequest, isStarted) =>
            crankshaft.canRevertDeploymentRequest(deploymentRequest, isStarted)
              .recover { case e: RejectingError => throw e.copy(s"Cannot revert the request #${deploymentRequest.id}: ${e.msg}") }
              .flatMap { _ =>
                crankshaft.findExecutionSpecificationsForRevert(deploymentRequest).map { case (undetermined, determined) =>
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

  post("/api/deployment-requests/:id/actions/deploy") { r: RequestWithId =>
    authenticate(r.request) { case user =>
      withIdAndRequest(
        (id, _: RequestWithId) =>
          engine
            .deploy(user, id)
            .map(_.map(_ => Map("id" -> id))),
        5.seconds
      )(r)
    }
  }

  post("/api/deployment-requests/:id/actions/revert") { r: RequestWithIdAndDefaultVersion =>
    authenticate(r.request) { case user =>
      withIdAndRequest(
        (id, _: RequestWithIdAndDefaultVersion) => {
          crankshaft.isDeploymentRequestStarted(id)
            .flatMap(
              _.map { case (deploymentRequest, isStarted) =>
                if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
                  throw ForbiddenException()

                crankshaft
                  .canRevertDeploymentRequest(deploymentRequest, isStarted)
                  .flatMap(_ => crankshaft.revert(id, user.name, r.defaultVersion.map(Version.apply)))
                  .map(_.map(_ => Map("id" -> id)))
              }.getOrElse(Future.successful(None))
            )
        },
        5.seconds
      )(r)
    }
  }

  /**
    * Warning: this route may return HTTP 200 OK with a list of error messages in the body!
    * These errors are about individual executions that could not be stopped, while another
    * key in the body gives the number of executions that have been successfully stopped.
    * No information is returned about the executions that were already stopped.
    */
  post("/api/deployment-requests/:id/actions/stop") { r: RequestWithId =>
    authenticate(r.request) { case user =>
      withIdAndRequest(
        (id, _: RequestWithId) => {
          crankshaft.findDeepDeploymentRequestById(id)
            .flatMap(_
              .map { deploymentRequest =>
                if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
                  throw ForbiddenException()

                crankshaft
                  .tryStopDeploymentRequest(deploymentRequest, user.name)
                  .map { case (nbStopped, errorMessages) =>
                    Some(Map(
                      "id" -> id,
                      "stopped" -> nbStopped,
                      "failures" -> errorMessages
                    ))
                  }
              }
              .getOrElse(Future.successful(None))
            )
        },
        5.seconds
      )(r)
    }
  }

  get("/api/deployment-requests/:id/execution-traces")(
    withLongId(
      crankshaft.findExecutionTracesByDeploymentRequest,
      5.seconds
    )
  )

  /**
    * Respond a:
    * - HTTP 204 if the update is a success
    * - HTTP 404 if the execution trace doesn't exist
    * - HTTP 422 if the execution state's transition is unsupported
    */
  put(RestApi.executionCallbackPath(":id")) {
    // todo: give the permissions to actual executors
    withIdAndRequest(
      (id, r: ExecutionTracePut) => {
        val executionState =
          try {
            ExecutionState.withName(r.state)
          } catch {
            case _: NoSuchElementException => throw BadRequestException(s"Unknown state `${r.state}`")
          }
        val statusMap: Map[String, TargetAtomStatus] =
          r.targetStatus.map { case (atom, status) => // don't use mapValues, as it gives a view (lazy generation, incompatible with error management here)
            try {
              (atom, TargetAtomStatus(Status.withName(status("code")), status("detail")))
            } catch {
              case _: NoSuchElementException =>
                throw BadRequestException(s"Bad target status for `$atom`: ${status.map { case (k, v) => s"$k='$v'" }.mkString(", ")}")
            }
          }
        crankshaft.tryUpdateExecutionTrace(id, executionState, r.detail, r.logHref.headOption.map(_ => r.logHref), statusMap)
          .map(_.map(_ => response.noContent))
      },
      3.seconds
    )
  }

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
        .map(crankshaft.computeState)
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
          targetStatus
            .map(ts => ts.targetAtom -> Map("code" -> ts.code.toString, "detail" -> ts.detail))
            .toMap
        },
        "executions" -> executionTraces
      ) ++ op.closingDate.map("closingDate" -> _)
    }

  get("/api/unstable/deployment-requests/:id") { r: RequestWithId =>
    withLongId(
      crankshaft
        .findDeepDeploymentRequestAndEffects(_)
        .flatMap(_.map { case (deploymentRequest, sortedEffects) =>
          val targets = deploymentRequest.parsedTarget.select
          val isAdmin = r.request.user.exists(user =>
            permissions.isAuthorized(user, GeneralAction.administrate)
          )

          // todo: a future workflow will differentiate requests and applies
          def authorized(op: Operation.Kind) = r.request.user.exists(user =>
            permissions.isAuthorized(user, DeploymentAction.applyOperation, op, deploymentRequest.product.name, targets)
          )

          Future
            .sequence(
              Operation.values.map(action =>
                (action match {
                  case Operation.deploy => crankshaft.canDeployDeploymentRequest(deploymentRequest)
                  case Operation.revert => crankshaft.canRevertDeploymentRequest(deploymentRequest, sortedEffects.nonEmpty)
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
          crankshaft.queryDeepDeploymentRequests(r.where, r.orderBy, r.limit, r.offset)
            .map(_.map { case (deploymentRequest, lastOperationEffect) =>
              serialize(deploymentRequest, lastOperationEffect)
            })
        } catch {
          case e: IllegalArgumentException => throw BadRequestException(e.getMessage)
        }
      },
      5.seconds)
  }

  get("/api/version") { _: Request =>
    response.ok.plain(RestApi.perpetuoVersion)
  }

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    throw NotFoundException()
  }

  get("/api/unstable/db/locks/drop-em-all") { r: Request =>
    authenticate(r) { case user if permissions.isAuthorized(user, GeneralAction.administrate) =>
      timeBoxed(
        crankshaft.dbBinding.releaseAllLocks().map(count => Some(Map("deleted" -> count))),
        5.seconds
      )
    }
  }

  // TODO: remove <<
  get("/api/unstable/products/actions/count-unreferenced") { r: Request =>
    timeBoxed(crankshaft.dbBinding.countUnreferencedProducts(), 5.seconds)
  }

  post("/api/unstable/products/actions/delete-unreferenced") { r: Request =>
    authenticate(r) { case user if permissions.isAuthorized(user, GeneralAction.administrate) =>
      timeBoxed(
        crankshaft.dbBinding.deleteUnreferencedProducts().map(count => Some(Map("deleted" -> count))),
        5.seconds
      )
    }
  }
  // >>
}

object RestApi {
  val perpetuoVersion: String = AppConfigProvider.config.getString("perpetuo.version")
  val selfUrl: String = AppConfigProvider.config.getString("selfUrl")

  def executionCallbackPath(execTraceId: String): String = s"/api/execution-traces/$execTraceId"

  def executionCallbackUrl(execTraceId: Long): String = selfUrl + executionCallbackPath(execTraceId.toString)
}
