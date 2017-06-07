package com.criteo.perpetuo.app

import java.sql.SQLException
import javax.inject.Inject

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dao.UnknownProduct
import com.criteo.perpetuo.dispatchers.Execution
import com.criteo.perpetuo.model.DeploymentRequestParser.parse
import com.criteo.perpetuo.model._
import com.twitter.finagle.http.{Request, Response, Status => HttpStatus}
import com.twitter.finatra.http.exceptions.{BadRequestException, ConflictException, HttpException}
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import com.twitter.util.{Future => TwitterFuture}
import spray.json.JsonParser.ParsingException
import spray.json._

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
class RestController @Inject()(val execution: Execution)
  extends BaseController {

  private val plugins = new Plugins(execution.dbBinding)
  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")
  private val deployBotName = "qabot"
  private val escalationTeamNames = List(
    "d.caroff", "e.peroumalnaik", "g.bourguignon", "m.runtz", "m.molongo",
    "m.nguyen", "m.soltani", "s.guerrier", "t.zhuang"
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

  get("/api/products") { _: Request =>
    timeBoxed(
      execution.dbBinding.getProductNames,
      2.seconds
    )
  }

  post("/api/products") { r: ProductPost =>
    authenticate(r.request) { case user if user.name == deployBotName =>
      timeBoxed(
        execution.dbBinding.insert(r.name)
          .recover {
            case e: SQLException if e.getMessage.contains("nique index") =>
              // there is no specific exception type if the name is already used but the error message starts with
              // * if H2: Unique index or primary key violation: "ix_product_name ON PUBLIC.""product""(""name"") VALUES ('my product', 1)"
              // * if SQLServer: Cannot insert duplicate key row in object 'dbo.product' with unique index 'ix_product_name'
              throw ConflictException(s"Name `${r.name}` is already used")
          }
          .map(_ => Some(response.created.nothing)),
        2.seconds
      )
    }
  }

  post("/api/products/last-version") { r: ProductPost =>
    handleTimeout(
      plugins.externalData.lastValidVersion(r.name)
    )
  }

  post("/api/products/validate-version") { r: ProductPostWithVersion =>
    handleTimeout(
      plugins.externalData.validateVersion(r.name, r.version)
    )
  }

  get("/api/deployment-requests/:id")(
    withLongId(
      execution.dbBinding.findDeploymentRequestByIdWithProduct(_).map(_.map(_.toJsonReadyMap)),
      2.seconds
    )
  )

  post("/api/deployment-requests") { r: Request =>
    val autoStart = r.getBooleanParam("start", default = false)
    authenticate(r) { case user if user.name == deployBotName || !autoStart =>
      timeBoxed(
        {
          val attrs = try {
            parse(r.contentString, user.name)
          }
          catch {
            case e: ParsingException => throw BadRequestException(e.getMessage)
          }

          if (AppConfig.transition && !autoStart) {
            execution.dbBinding.findProductByName(attrs.productName)
              .map(_.map(DeploymentRequest(0, _, attrs.version, attrs.target, attrs.comment, attrs.creator, attrs.creationDate))
                .getOrElse {
                  throw BadRequestException(s"Product `${attrs.productName}` could not be found")
                })
              .flatMap(plugins.hooks.onDeploymentRequestCreated(_, immediateStart = autoStart, r.contentString))
              .map(ticketUrl =>
                Some(response.created.json(Map("ticketUrl" -> ticketUrl)))
              )
          }
          else {
            // first, log the user's general intent
            val futureDepReq = execution.dbBinding.insert(attrs)
            // when the record is created, notify the corresponding hook
            futureDepReq.foreach(plugins.hooks.onDeploymentRequestCreated(_, immediateStart = autoStart, r.contentString))

            if (autoStart) {
              futureDepReq.flatMap(depReq =>
                execution.startOperation(plugins.dispatcher, depReq, Operation.deploy, user.name).map {
                  case (started, failed) => (depReq, started, failed)
                }
              ).foreach { case (depReq, started, failed) =>
                plugins.hooks.onDeploymentRequestStarted(depReq, started, failed, immediately = true)
              }
            }

            futureDepReq
              .recover { case e: UnknownProduct => throw BadRequestException(s"Product `${e.productName}` could not be found") }
              .map { depReq => Some(response.created.json(Map("id" -> depReq.id))) }
          }
        },
        2.seconds
      )
    }
  }

  put("/api/deployment-requests/:id") { r: RequestWithId =>
    // todo: Use AD group to give escalation team permissions to deploy in prod
    authenticate(r.request) { case user if user.name == deployBotName || escalationTeamNames.contains(user.name) || AppConfig.env != "prod" =>
      withIdAndRequest(
        (id, _: RequestWithId) => {
          execution.dbBinding.findDeploymentRequestByIdWithProduct(id).map(_.map { req =>
            // done asynchronously
            execution.startOperation(plugins.dispatcher, req, Operation.deploy, user.name)
              .foreach { case (started, failed) =>
                plugins.hooks.onDeploymentRequestStarted(req, started, failed, immediately = false)
              }

            // returned synchronously
            response.ok.json(Map("id" -> req.id))
          })
        },
        2.seconds
      )(r)
    }
  }

  get("/api/execution-traces/by-deployment-request/:id")(
    withLongId(id =>
      execution.dbBinding.findExecutionTracesByDeploymentRequest(id).flatMap { traces =>
        if (traces.isEmpty) {
          // if there is a deployment request with that ID, return the empty list, otherwise a 404
          execution.dbBinding.deploymentRequestExists(id).map(if (_) Some(traces) else None)
        }
        else
          Future.successful(Some(traces))
      },
      2.seconds
    )
  )

  private def putExecutionTrace(id: Long, r: ExecutionTracePut) = {
    val executionState = try {
      ExecutionState.withName(r.state)
    } catch {
      case _: NoSuchElementException => throw BadRequestException(s"Unknown state `${r.state}`")
    }

    import DefaultJsonProtocol._
    val statusMap =
      try {
        r.targetStatus.map { // don't use mapValues, as it gives a view (lazy generation, incompatible with error management here)
          case (k, obj) => (k, Status.targetMapJsonFormat.read(obj.toJson)) // yes it's crazy to use spray's case class deserializer
        }
      } catch {
        case e: DeserializationException => throw BadRequestException(e.getMessage)
      }

    val executionUpdate = if (r.logHref.nonEmpty)
      execution.dbBinding.updateExecutionTrace(id, r.logHref, executionState)
    else
      execution.dbBinding.updateExecutionTrace(id, executionState)

    val op = if (statusMap.nonEmpty) {
      executionUpdate.flatMap {
        if (_) {
          // the execution trace has been updated, so it must exist!
          execution.dbBinding.findExecutionTraceById(id).map(_.get).flatMap { execTrace =>
            val op = execTrace.operationTrace
            execution.dbBinding.updateOperationTrace(op.id, op.partialUpdate(statusMap))
              .map { updated =>
                assert(updated)
                Some()
              }
          }
        }
        else
          Future.successful(None)
      }
    }
    else
      executionUpdate.map {
        if (_) Some() else None
      }

    op.map(_.map(_ => response.noContent))
  }

  put("/api/execution-traces/:id") {
    // todo: give the permission to Rundeck only
    withIdAndRequest(
      putExecutionTrace,
      3.seconds
    )
  }

  get("/api/operation-traces/by-deployment-request/:id")(
    withLongId(id =>
      execution.dbBinding.findOperationTracesByDeploymentRequest(id).flatMap { traces =>
        if (traces.isEmpty) {
          // if there is a deployment request with that ID, return the empty list, otherwise a 404
          execution.dbBinding.deploymentRequestExists(id).map(if (_) Some(traces) else None)
        }
        else
          Future.successful(Some(traces))
      },
      2.seconds
    )
  )

  get("/api/unstable/deployment-requests") { _: Request =>
    timeBoxed(
      execution.dbBinding.deepQueryDeploymentRequests(where = Seq(), orderBy = Seq(), limit = 20, offset = 0),
      5.seconds
    )
  }
  get("/api/unstable/deployment-requests/:id")(
    withLongId(
      id => execution.dbBinding.deepQueryDeploymentRequests(id),
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
          execution.dbBinding.deepQueryDeploymentRequests(r.where, r.orderBy, r.limit, r.offset)
        } catch {
          case e: IllegalArgumentException => throw BadRequestException(e.getMessage)
        }
      },
      5.seconds)
  }

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    response.notFound
  }

}
