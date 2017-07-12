package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.auth.UserFilter._
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.dao.UnknownProduct
import com.criteo.perpetuo.dispatchers.Execution
import com.criteo.perpetuo.engine.Engine
import com.twitter.finagle.http.{Request, Response, Status => HttpStatus}
import com.twitter.finatra.http.exceptions.{BadRequestException, HttpException}
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import com.twitter.util.{Future => TwitterFuture}

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
    "d.caroff", "e.peroumalnaik", "g.bourguignon", "m.runtz", "m.molongo",
    "m.nguyen", "m.soltani", "s.guerrier", "t.tellier", "t.zhuang"
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
      engine.getProductNames,
      2.seconds
    )
  }

  post("/api/products") { r: ProductPost =>
    authenticate(r.request) { case user if user.name == deployBotName =>
      timeBoxed(
        engine.insertProduct(r.name)
          .map(_ => Some(response.created.nothing)),
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
    handleTimeout(
      engine.validateVersion(r.name, r.version)
    )
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
        engine.createDeploymentRequest(user.name, r.contentString, autoStart)
          .map(x => Some(response.created.json(x)))
          .recover {
            case e: UnknownProduct => throw BadRequestException(s"Product `${e.productName}` could not be found")
          },
        5.seconds // fixme: get back to 2 seconds when the hook will be called asynchronously
      )
    }
  }

  put("/api/deployment-requests/:id") { r: RequestWithId =>
    // todo: Use AD group to give escalation team permissions to deploy in prod
    authenticate(r.request) { case user if user.name == deployBotName || escalationTeamNames.contains(user.name) || AppConfig.env != "prod" =>
      withIdAndRequest(
        (id, _: RequestWithId) => engine.startDeploymentRequest(id, user.name).map(_.map(response.ok.json)),
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
      (id, r: ExecutionTracePut) => engine.updateExecutionTrace(id, r.state, r.logHref, r.targetStatus).map(_.map(_ => response.noContent)),
      3.seconds
    )
  }

  get("/api/operation-traces/by-deployment-request/:id")(
    withLongId(
      engine.findOperationTracesByDeploymentRequest,
      2.seconds
    )
  )

  get("/api/unstable/deployment-requests") { _: Request =>
    timeBoxed(
      engine.queryDeepDeploymentRequests(where = Seq(), orderBy = Seq(), limit = 20, offset = 0),
      5.seconds
    )
  }
  get("/api/unstable/deployment-requests/:id")(
    withLongId(
      engine.getDeepDeploymentRequest,
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


object RestApi {
  def executionCallbackPath(executionId: String): String = s"/api/execution-traces/$executionId"
}
