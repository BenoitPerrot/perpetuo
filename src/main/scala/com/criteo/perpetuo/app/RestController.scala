package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.auth.{Authenticator, User}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.engine.{DeploymentRequestStatus, DeploymentStatus, Engine}
import com.criteo.perpetuo.model._
import com.twitter.finagle.http.Request
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
import scala.io.Source
import scala.util.Try

trait WithId {
  val id: String
}

trait WithRequest {
  val request: Request
}


@JsonIgnoreBody
private case class RequestWithId(@RouteParam @NotEmpty id: String,
                                 @Inject request: Request) extends WithId with WithRequest

private case class RequestWithNameAndActive(@NotEmpty name: String,
                                            active: Boolean = true,
                                            @Inject request: Request) extends WithRequest

private case class RequestWithNames(names: Seq[String],
                                    @Inject request: Request) extends WithRequest

private case class DeploymentActionRequest(@RouteParam @NotEmpty id: String,
                                           operationCount: Option[Int],
                                           @Inject request: Request) extends WithId with WithRequest

private case class RevertRequest(@RouteParam @NotEmpty id: String,
                                 operationCount: Option[Int],
                                 @NotEmpty defaultVersion: Option[String],
                                 @Inject request: Request) extends WithId with WithRequest

private case class ExecutionTracePut(@RouteParam @NotEmpty id: String,
                                     @NotEmpty state: String,
                                     detail: String = "",
                                     href: String = "",
                                     targetStatus: Map[String, Map[String, String]] = Map(),
                                     @Inject request: Request) extends WithId with WithRequest

private case class FilteringPost(where: Seq[Map[String, Any]] = Seq(),
                                 limit: Int = 20,
                                 offset: Int = 0,
                                 @Inject request: Request) extends WithRequest

/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val engine: Engine)
  extends BaseController
    with Authenticator
    with ExceptionsToHttpStatusTranslation {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")

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

  private def post[R <: WithId with WithRequest, O: Manifest](route: String, maxDuration: Duration)(callback: (Long, R, User) => Future[Option[O]])(implicit m: Manifest[R]): Unit = {
    post(route) { r: R =>
      authenticate(r.request) { case user =>
        withIdAndRequest((id: Long, r: R) => callback(id, r, user), maxDuration)(r)
      }
    }
  }

  get("/api/products") { _: Request =>
    timeBoxed(
      engine.getProducts,
      5.seconds
    )
  }

  put("/api/products") { r: RequestWithNameAndActive =>
    authenticate(r.request) { case user =>
      timeBoxed(
        engine
          .upsertProduct(user, r.name, r.active)
          .map(_ => Some(response.created.nothing)),
        5.seconds
      )
    }
  }

  post("/api/products") { r: RequestWithNames =>
    authenticate(r.request) { case user =>
      timeBoxed(
        engine
          .setActiveProducts(user, r.names)
          .map(products => Some(products)),
        5.seconds
      )
    }
  }

  get("/api/products/actions") { r: Request =>
     futurePool { if (r.user.exists(user => engine.allowedToUpdateProduct(user))) Seq("updateProduct") else Seq() }
  }

  post("/api/deployment-requests") { r: Request =>
    authenticate(r) { case user =>
      val protoDeploymentRequest = try {
        DeploymentRequestParser.parse(r.contentString, user.name)
      } catch {
        case e: ParsingException => throw BadRequestException(e.getMessage)
      }

      timeBoxed(
        engine.requestDeployment(user, protoDeploymentRequest)
          .map(x => Some(response.created.json(Map("id" -> x.id)))),
        5.seconds
      )
    }
  }

  post("/api/deployment-requests/:id/actions/devise-revert-plan") { r: RequestWithId =>
    withIdAndRequest(
      (id, _: RequestWithId) => {
        engine
          .deviseRevertPlan(id)
          .map(_.map { case (undetermined, determined) =>
            Some(Map(
              "undetermined" -> undetermined,
              "determined" -> determined.toStream.map { case (execSpec, targets) =>
                Map("version" -> execSpec.version, "targetAtoms" -> targets)
              }
            ))
          })
      },
      5.seconds
    )(r)
  }

  post("/api/deployment-requests/:id/actions/step", 5.seconds) { (id: Long, r: DeploymentActionRequest, user: User) =>
    engine
      .step(user, id, r.operationCount)
      .map(_.map(_ => Map("id" -> id)))
  }

  // TODO: Remove once clients have migrated to step
  post("/api/deployment-requests/:id/actions/deploy", 5.seconds) { (id: Long, _: RequestWithId, user: User) =>
    engine
      .step(user, id, None)
      .map(_.map(_ => Map("id" -> id)))
  }

  post("/api/deployment-requests/:id/actions/revert", 5.seconds) { (id: Long, r: RevertRequest, user: User) =>
    engine
      .revert(user, id, r.operationCount, r.defaultVersion.map(Version.apply))
      .map(_.map(_ => Map("id" -> id)))
  }
  // >>

  /**
    * Warning: this route may return HTTP 200 OK with a list of error messages in the body!
    * These errors are about individual executions that could not be stopped, while another
    * key in the body gives the number of executions that have been successfully stopped.
    * No information is returned about the executions that were already stopped.
    */
  post("/api/deployment-requests/:id/actions/stop", 5.seconds) { (id: Long, r: DeploymentActionRequest, user: User) =>
    engine
      .stop(user, id, r.operationCount)
      .map(_.map { case (nbStopped, errorMessages) =>
        Map(
          "id" -> id,
          "stopped" -> nbStopped,
          "failures" -> errorMessages
        )
      })
  }

  get("/api/deployment-requests/:id/execution-traces")(
    withLongId(
      engine.findExecutionTracesByDeploymentRequest,
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
        engine.tryUpdateExecutionTrace(id, executionState, r.detail, r.href.headOption.map(_ => r.href), statusMap)
          .map(_.map(_ => response.noContent))
      },
      3.seconds
    )
  }

  private def serialize(deploymentPlan: DeploymentPlan, deploymentStatus: DeploymentStatus.Value, lastOperationKind: Option[Operation.Kind]): Map[String, Any] =
    Map(
      "id" -> deploymentPlan.deploymentRequest.id,
      "comment" -> deploymentPlan.deploymentRequest.comment,
      "creationDate" -> deploymentPlan.deploymentRequest.creationDate,
      "creator" -> deploymentPlan.deploymentRequest.creator,
      "version" -> deploymentPlan.deploymentRequest.version,
      "plan" -> deploymentPlan.steps,
      "productName" -> deploymentPlan.deploymentRequest.product.name,
      "status" -> deploymentStatus,
      "lastOperationKind" -> lastOperationKind
    )

  private def serialize(status: DeploymentRequestStatus): Map[String, Any] =
    serialize(
      DeploymentPlan(status.deploymentRequest, status.deploymentPlanSteps),
      status.lastOperationStatus.map { case (_, opStatus) => DeploymentStatus.from(opStatus) }.getOrElse(DeploymentStatus.notStarted),
      status.lastOperationStatus.map { case (kind, _) => kind }
    ) ++
      Map("operations" ->
        status.operationEffects.map { case effect@OperationEffect(op, planStepIds, executionTraces, targetStatus) =>
          val getTargetDetail = if (effect.state == OperationEffectState.inProgress)
            (ts: TargetStatus) => if (ts.code == Status.notDone && ts.detail.isEmpty) "pending" else ts.detail
          else
            (_: TargetStatus).detail

          Map(
            "id" -> op.id,
            "planStepIds" -> planStepIds,
            "kind" -> op.kind.toString,
            "creator" -> op.creator,
            "creationDate" -> op.creationDate,
            "targetStatus" -> {
              targetStatus
                .map(ts => ts.targetAtom -> Map("code" -> ts.code.toString, "detail" -> getTargetDetail(ts)))
                .toMap
            },
            "status" -> effect.state,
            "executions" -> executionTraces
          ) ++ op.closingDate.map("closingDate" -> _)
        }
      ) ++
      Map("actions" -> status.eligibleActions.map { case (actionName, rejectionCause) =>
        Map("type" -> actionName) ++ rejectionCause.map("rejected" -> _)
      })

  get("/api/unstable/deployment-requests/:id") { r: RequestWithId =>
    withLongId(
      id => engine.queryDeploymentRequestStatus(r.request.user, id).map(_.map(serialize)),
      5.seconds
    )(r)
  }
  post("/api/unstable/deployment-requests") { r: FilteringPost =>
    timeBoxed(
      {
        if (1000 < r.limit) {
          throw BadRequestException("`limit` shall be lower than 1000")
        }
        try {
          engine.findDeploymentRequestsWithStatuses(r.where, r.limit, r.offset)
            .map(_.map { case (deploymentPlan, deploymentStatus, lastOperationKind) =>
              serialize(deploymentPlan, deploymentStatus, lastOperationKind)
            })
        } catch {
          case e: IllegalArgumentException => throw BadRequestException(e.getMessage)
        }
      },
      5.seconds)
  }

  get("/api/deployment-requests/:id/state") { r: RequestWithId =>
    withLongId(
      id => engine.findDeploymentRequestState(id).map(_.map { deploymentRequestState =>
        val state = deploymentRequestState.getClass.getSimpleName
        Some(Map("state" -> Character.toLowerCase(state.charAt(0)).toString.concat(state.substring(1))))
      }),
      5.seconds
    )(r)
  }

  get("/api/version") { _: Request =>
    response.ok.plain(RestApi.perpetuoVersion)
  }

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    throw NotFoundException()
  }
}

object RestApi {
  val perpetuoVersion: String = Source.fromInputStream(getClass.getResourceAsStream("/version")).mkString
  val selfUrl: String = AppConfigProvider.config.getString("selfUrl")

  def executionCallbackPath(execTraceId: String): String = s"/api/execution-traces/$execTraceId"

  def executionCallbackUrl(execTraceId: Long): String = selfUrl + executionCallbackPath(execTraceId.toString)
}
