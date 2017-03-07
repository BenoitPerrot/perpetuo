package com.criteo.perpetuo.app

import java.sql.SQLException
import javax.inject.Inject

import com.criteo.perpetuo.dao.UnknownProduct
import com.criteo.perpetuo.dispatchers.{Execution, TargetDispatcher}
import com.criteo.perpetuo.model.DeploymentRequestParser.parse
import com.criteo.perpetuo.model.{ExecutionState, Operation, TargetStatus}
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.exceptions.{BadRequestException, ConflictException}
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import spray.json.JsonParser.ParsingException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


@JsonIgnoreBody
private case class GetWithId(@RouteParam @NotEmpty id: String)

private case class ProductPost(@NotEmpty name: String)

private case class ExecutionTracePut(@RouteParam @NotEmpty id: String,
                                     @NotEmpty state: String,
                                     logHref: String = "",
                                     targetStatus: Map[String, String] = Map())


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val execution: Execution)
  extends BaseController {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")
  private val dispatcher = TargetDispatcher.fromConfig

  private def timeBoxed[T](view: => Future[T], maxDuration: Duration): com.twitter.util.Future[T] =
    futurePool {
      Await.result(view, maxDuration)
    }

  private def withLongId[T](view: Long => Future[Option[T]], maxDuration: Duration): GetWithId => com.twitter.util.Future[Option[T]] =
    request => futurePool {
      Try(request.id.toLong).toOption.map(view).flatMap(Await.result(_, maxDuration))
    }


  get("/api/products") { _: Request =>
    timeBoxed(
      execution.dbBinding.getProductNames,
      2.seconds
    )
  }

  post("/api/products") { r: ProductPost =>
    timeBoxed(
      execution.dbBinding.insert(r.name)
        .recover {
          case e: SQLException if e.getMessage.contains("nique index") =>
            // there is no specific exception type if the name is already used but the error message starts with
            // * if H2: Unique index or primary key violation: "ix_product_name ON PUBLIC.""product""(""name"") VALUES ('my product', 1)"
            // * if SQLServer: Cannot insert duplicate key row in object 'dbo.product' with unique index 'ix_product_name'
            throw ConflictException(s"Name `${r.name}` is already used")
        }
        .map(_ => response.created.nothing),
      2.seconds
    )
  }

  get("/api/deployment-requests/:id")(
    withLongId(
      execution.dbBinding.findDeploymentRequestByIdWithProduct(_).map(_.map(_.toJsonReadyMap)),
      2.seconds
    )
  )

  post("/api/deployment-requests") { r: Request =>
    timeBoxed(
      {
        val attrs = try {
          parse(r.contentString)
        }
        catch {
          case e: ParsingException => throw BadRequestException(e.getMessage)
        }

        // first, log the user's general intent
        val futureDepReq = execution.dbBinding.insert(attrs)

        if (r.getBooleanParam("start", default = false)) {
          val asyncStart = futureDepReq.flatMap(execution.startOperation(dispatcher, _, Operation.deploy))

          asyncStart.onFailure { case e => logger.error("Transaction failed to start: " + e.getMessage + "\n" + e.getStackTrace.mkString("\n")) }
        }

        futureDepReq
          .recover { case e: UnknownProduct => throw BadRequestException(s"Product `${e.productName}` could not be found") }
          .map { depReq => response.created.json(Map("id" -> depReq.id)) }
      },
      2.seconds
    )
  }

  put("/api/deployment-requests/:id")(
    withLongId(
      execution.dbBinding.findDeploymentRequestByIdWithProduct(_).map(_.map { req =>
        // done asynchronously
        execution.startOperation(dispatcher, req, Operation.deploy)

        // returned synchronously
        Map("id" -> req.id)
      }),
      2.seconds
    )
  )

  get("/api/execution-traces/by-deployment-request/:id")(
    withLongId(id =>
      execution.dbBinding.findExecutionTraceRecordsByDeploymentRequest(id).flatMap { traces =>
        if (traces.isEmpty) {
          // if there is a deployment request with that ID, return the empty list, otherwise a 404
          execution.dbBinding.deploymentRequestExists(id).map(if (_) Some(traces) else None)
        }
        else
          Future.successful(
            Some(
              traces.map(trace =>
                Map(
                  "id" -> trace.id.get,
                  "logHref" -> trace.logHref,
                  "state" -> trace.state.toString
                )
              )
            )
          )
      },
      2.seconds
    )
  )

  put("/api/execution-traces/:id") {
    r: ExecutionTracePut =>
      futurePool {
        Try(r.id.toLong).toOption
          .map { id =>
            val executionState = try {
              ExecutionState.withName(r.state)
            } catch {
              case _: NoSuchElementException => throw BadRequestException(s"Unknown state `${r.state}`")
            }

            val statusMap = r.targetStatus.mapValues { status =>
              try {
                TargetStatus.withName(status)
              } catch {
                case _: NoSuchElementException => throw BadRequestException(s"Unknown target status `$status`")
              }
            }

            val executionUpdate = if (r.logHref.nonEmpty)
              execution.dbBinding.updateExecutionTrace(id, r.logHref, executionState)
            else
              execution.dbBinding.updateExecutionTrace(id, executionState)

            if (r.targetStatus.nonEmpty) {
              executionUpdate.flatMap {
                if (_) {
                  // the execution trace has been updated, so it must exist!
                  execution.dbBinding.findExecutionTraceByIdWithOperationTrace(id).map(_.get).flatMap { execTrace =>
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
          }
          .flatMap(Await.result(_, 3.seconds))
          .map(_ => response.noContent)
          .getOrElse(response.notFound)
      }
  }

  get("/api/unstable/deployment-requests") { _: Request =>
    timeBoxed(
      execution.dbBinding.deepQueryDeploymentRequests(),
      5.seconds
    )
  }

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { _: Request =>
    response.notFound
  }

}
