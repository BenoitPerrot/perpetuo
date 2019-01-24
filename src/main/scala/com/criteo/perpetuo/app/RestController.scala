package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.auth.{Authenticator, GeneralAction, User}
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.engine.{DeploymentState, Engine}
import com.criteo.perpetuo.model._
import com.jakehschwartz.finatra.swagger.SwaggerController
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.exceptions._
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import com.twitter.util.{Future => TwitterFuture}
import io.swagger.models.{Operation, Swagger}
import javax.inject.{Inject, Singleton}
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

private case class RequestWithProductName(@NotEmpty productName: String,
                                          @Inject request: Request) extends WithRequest

/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(engine: Engine, restApi: RestApi, swag: Swagger)
  extends BaseController
    with Authenticator
    with ExceptionsToHttpStatusTranslation
    with SwaggerController {

  override protected implicit val swagger: Swagger = swag

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")

  private def timeBoxed[T](view: => Future[T], maxDuration: Duration): TwitterFuture[T] =
    futurePool(
      await(view, maxDuration)
    )

  private def withLongId[T](view: Long => Future[Option[T]], maxDuration: Duration): RequestWithId => TwitterFuture[Option[T]] =
    withIdAndRequest[RequestWithId, T]({ case (id, _) => view(id) }, maxDuration)

  private def withIdAndRequest[I <: WithId, O](view: (Long, I) => Future[Option[O]], maxDuration: Duration): I => TwitterFuture[Option[O]] =
    request => futurePool(
      Try(request.id.toLong).toOption.map(view(_, request)).flatMap(await(_, maxDuration))
    )

  private def postWithDoc[R <: WithId with WithRequest, O: Manifest]
    (route: String, maxDuration: Duration)
    (doc: Operation => Operation)
    (callback: (Long, R, User) => Future[Option[O]])
    (implicit m: Manifest[R]): Unit = {

    postWithDoc(route)(doc) { r: R =>
      authenticate(r.request) { case user =>
        withIdAndRequest((id: Long, r: R) => callback(id, r, user), maxDuration)(r)
      }
    }
  }

  getWithDoc("/api/products") {_
      .summary("List all registered products")
      .tag("Product")
      .responseWith[Array[Product]](200, "The list of all the products is returned", Some(Array(Product(123, "product name", active = true))))
  } { _: Request =>
    timeBoxed(
      engine.getProducts,
      5.seconds
    )
  }

  putWithDoc("/api/products") {_
      .summary("Update or insert a product with the given name and activeness")
      .tag("Product")
      .bodyParam[String]("name", "name of the product", Some("product1"))
      .bodyParam[Boolean]("active", "activeness of the product", Some(true))
      .responseWith(201, "No content is returned")
  } { r: RequestWithNameAndActive =>
    authenticate(r.request) { case user =>
      timeBoxed(
        engine
          .upsertProduct(user, r.name, r.active)
          .map(_ => Some(response.created.nothing)),
        5.seconds
      )
    }
  }

  postWithDoc("/api/products") {_
      .summary("Activate products by name")
      .tag("Product")
      .description(
        """- Activate the products whose names are in the given list, inserting new products when name is unknown.
          |- Deactivate the registered products whose names are not in the given list.
        """.stripMargin)
      .bodyParam[Array[String]]("names", "Array of product names", Some(Array("product1", "product2")))
      .responseWith[Array[Product]](200, "All created products and updated ones are returned (i.e excluding unmodified ones)")
  } { r: RequestWithNames =>
    authenticate(r.request) { case user =>
      timeBoxed(
        engine
          .setActiveProducts(user, r.names)
          .map(products => Some(products)),
        5.seconds
      )
    }
  }

  postWithDoc("/api/products/version-per-target") {_
      .summary("List the deployed versions of the given product on a per target basis")
      .tag("Product")
      .bodyParam[String]("name", "name of a product", Some("product1"))
      .responseWith[Map[TargetAtom, Version]](200, "Map associating a target to a version is returned")
  } { r: RequestWithProductName =>
    timeBoxed(
      engine.getCurrentVersionPerTarget(r.productName),
      5.seconds
    )
  }

  getWithDoc("/api/actions") {_
      .summary("List the authorized admin actions for this user")
      .description(s"the returned actions can be : ${GeneralAction.values.mkString(", ")}")
      .tag("Action")
      .responseWith[GeneralAction.Value](200, "List of allowed actions")
  } { r: Request =>
    futurePool(
      r.user.map(user => engine.getAllowedActions(user)).getOrElse(Seq())
    )
  }

  postWithDoc("/api/deployment-requests") {_
      .summary("Request a deployment")
      .tag("Deployment request")
      .bodyParam[String]("deployment request", "A deployment request")
      .responseWith(200, "The id of the deployment request that was created")
      .responseWith(400, "When an error occurred, e.g while parsing the input")
  } { r: Request =>
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

  private def deploymentRequestDoc(op: Operation): Operation = {
    op.tag("Deployment request")
      .routeParam[Int]("id", "deployment request id")
  }

  postWithDoc("/api/deployment-requests/:id/actions/devise-revert-plan") {
    deploymentRequestDoc(_)
      .summary("Get the revert plan for the given deployment request")
      .description(
        """Return the version that would be put back on each target impacted by the deployment request
          | NOTE: this is purely informative, no operation will be carried out on the targets
        """.stripMargin)
      .responseWith(200,
        """A map that will contain two entries:
          | - One for the undetermined targets i.e the targets for which it was not possible to determine the previous version
          | - One for the determined targets that will contain a map associating a version to all its targets""".stripMargin)
  } { r: RequestWithId =>
    withLongId(
      id => engine
        .deviseRevertPlan(id)
        .map(_.map { case (undetermined, determined) =>
          Some(Map(
            "undetermined" -> undetermined,
            "determined" -> determined.map { case (execSpec, targets) =>
              Map("version" -> execSpec.version, "targetAtoms" -> targets)
            }
          ))
        }),
      5.seconds
    )(r)
  }

  //Operation count start at zero after the deployment request was created
  postWithDoc("/api/deployment-requests/:id/actions/step", 5.seconds) {
    deploymentRequestDoc(_)
      .summary("Step forward the given deployment request")
      .description(
        """Deploys the product's version specified in the given deployment request on the targets covered by the next step of its plan. Retries the last attempted step if it failed.
          | The method is idempotent when an operationCount is specified.
          | Operation count start at zero after the deployment request was created.
        """.stripMargin)
      .bodyParam[Int]("operationCount",
      "Optional: number of operations performed so far for the given deployment request. If the actual operation count differs from the query, the step won't be carried out")
      .responseWith(200, "Corresponding operation id")
  } { (id: Long, r: DeploymentActionRequest, user: User) =>
    engine
      .step(user, id, r.operationCount)
      .map(_.map(_ => Map("id" -> id)))
  }

  postWithDoc("/api/deployment-requests/:id/actions/revert", 5.seconds) {
    deploymentRequestDoc(_)
      .summary("Revert the given deployment request")
      .description(
        """Deploys the product specified in the given deployment request on the targets that were impacted by the previous steps, using the previous latest products' version for each targets.
          | See also "/devise-revert-plan" to obtain the versions that will be used for each target.
          | The method is idempotent when an operationCount is specified.
        """.stripMargin)
      .queryParam[Int]("operationCount", required = false)
      .responseWith(200, "Corresponding operation id")
  } { (id: Long, r: RevertRequest, user: User) =>
    engine
      .revert(user, id, r.operationCount, r.defaultVersion.map(Version.apply))
      .map(_.map(_ => Map("id" -> id)))
  }
  // >>

  postWithDoc("/api/deployment-requests/:id/actions/stop", 5.seconds) {
    deploymentRequestDoc(_)
      .summary("Stop the given deployment request")
      .description(
        """Warning: this route may return HTTP 200 OK with a list of error messages in the body!
          | These errors are about individual executions that could not be stopped, while another
          | key in the body gives the number of executions that have been successfully stopped.
          | No information is returned about the executions that were already stopped.
        """.stripMargin)
      .responseWith(200, "id of the request with the number of stopped target and error messages")
  } { (id: Long, r: DeploymentActionRequest, user: User) =>
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

  postWithDoc("/api/deployment-requests/:id/actions/abandon", 5.seconds) {
    deploymentRequestDoc(_)
      .summary("Abandon the given deployment request")
      .responseWith(200, "id of the abandoned deployment request")
  } { (id: Long, _: RequestWithId, user: User) =>
    engine
      .abandon(user, id)
      .map(_.map(_ => Map("id" -> id)))
  }

  /**
    * Respond a:
    * - HTTP 204 if the update is a success
    * - HTTP 404 if the execution trace doesn't exist
    * - HTTP 422 if the execution state's transition is unsupported
    */
  put(restApi.executionCallbackPath(":id")) {
    // todo: give the permissions to actual executors
    withIdAndRequest(
      (id, r: ExecutionTracePut) => {
        val executionState =
          try {
            ExecutionState.withName(r.state)
          } catch {
            case _: NoSuchElementException => throw BadRequestException(s"Unknown state `${r.state}`")
          }
        val statusMap: Map[TargetAtom, TargetAtomStatus] =
          r.targetStatus.map { case (atom, status) => // don't use mapValues, as it gives a view (lazy generation, incompatible with error management here)
            try {
              (TargetAtom(atom), TargetAtomStatus(Status.withName(status("code")), status("detail")))
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

  private def serialize(deploymentRequest: DeploymentRequest, deploymentPlanSteps: Seq[DeploymentPlanStep], state: DeploymentState): Map[String, Any] = {
    val deploymentPlan = DeploymentPlan(deploymentRequest, deploymentPlanSteps)
    Map(
      "id" -> deploymentPlan.deploymentRequest.id,
      "comment" -> deploymentPlan.deploymentRequest.comment,
      "creationDate" -> deploymentPlan.deploymentRequest.creationDate,
      "creator" -> deploymentPlan.deploymentRequest.creator,
      "version" -> deploymentPlan.deploymentRequest.version,
      "plan" -> deploymentPlan.steps,
      "productName" -> deploymentPlan.deploymentRequest.product.name,
      "state" -> state.toString
    )
  }

  private def serialize(state: DeploymentState): Map[String, Any] =
    serialize(state.deploymentRequest, state.deploymentPlanSteps, state)

  private def serialize(state: DeploymentState, eligibleActions: Seq[(String, Option[String])]): Map[String, Any] = {
    serialize(state) ++
      state.outdatedBy.map(id => Map("outdatedBy" -> id)).getOrElse(Map.empty) ++
      Map("operations" ->
        state.effects.map { case effect@OperationEffect(op, planStepIds, executionTraces, targetStatus) =>
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
                .map(ts => ts.targetAtom.name -> Map("code" -> ts.code.toString, "detail" -> getTargetDetail(ts)))
                .toMap
            },
            "status" -> effect.state,
            "executions" -> executionTraces
          ) ++ op.closingDate.map("closingDate" -> _)
        }
      ) ++
      Map("actions" -> eligibleActions.map { case (actionName, rejectionCause) =>
        Map("type" -> actionName) ++ rejectionCause.map("rejected" -> _)
      })
  }

  get("/api/unstable/deployment-requests/:id") { r: RequestWithId =>
    withLongId(
      id => engine.queryDeploymentRequestStatus(r.request.user, id).map(_.map { case (state, eligibleActions) => serialize(state, eligibleActions) }),
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
          engine.findDeploymentRequestsStates(r.where, r.limit, r.offset)
            .map(_.map(serialize))
        } catch {
          case e: IllegalArgumentException => throw BadRequestException(e.getMessage)
        }
      },
      5.seconds)
  }

  getWithDoc("/api/deployment-requests/:id/state") {
    deploymentRequestDoc(_)
      .summary("Get the state of the given deployment request")
      .responseWith(200, "Deployment request state")
  } { r: RequestWithId =>
    withLongId(
      id => engine.findDeploymentRequestState(id).map(_.map { deploymentRequestState =>
        Some(Map("state" -> deploymentRequestState.toString))
      }),
      5.seconds
    )(r)
  }

  private val perpetuoVersion: String = Source.fromInputStream(getClass.getResourceAsStream("/version")).mkString

  get("/api/version") { _: Request =>
    response.ok.plain(perpetuoVersion)
  }

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    throw NotFoundException()
  }
}

@Singleton
class RestApi @Inject()(appConfig: AppConfig) {
  def executionCallbackPath(execTraceId: String): String = s"/api/execution-traces/$execTraceId"

  def executionCallbackUrl(execTraceId: Long): String = appConfig.selfUrl + executionCallbackPath(execTraceId.toString)
}
