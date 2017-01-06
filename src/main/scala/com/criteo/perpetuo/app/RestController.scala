package com.criteo.perpetuo.app

import java.sql.Timestamp
import javax.inject.Inject
import javax.sql.DataSource

import com.criteo.perpetuo.dao.{DeploymentRequest, DeploymentRequestBinding}
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.exceptions.BadRequestException
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.validation._
import slick.driver.H2Driver
import spray.json.{JsObject, JsString, _}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try


@JsonIgnoreBody
case class DeploymentRequestGet(@RouteParam @NotEmpty id: String)


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val dataSource: DataSource,
                               val deploymentRequests: DeploymentRequestBinding)
  extends BaseController {

  import deploymentRequests.profile.api._

  private val db = Database.forDataSource(dataSource)

  {
    // TODO: Remove once live
    val isEmbedded = deploymentRequests.profile.isInstanceOf[H2Driver]
    if (isEmbedded) {
      val schemaCreation = DBIO.seq(
        deploymentRequests.deploymentRequestQuery.schema.create
      )
      Await.result(db.run(schemaCreation), 2.second)
    }
  }

  private def missing(key: String) = throw BadRequestException(s"Expected to find `$key` at request root")

  private def parseTargetExpression(target: JsValue) =
    (target match {
      case string: JsString => Seq((JsArray(), Seq(string)))
      case arr: JsArray => arr.elements.map {
        case string: JsString => (JsArray(), Seq(string))
        case obj: JsObject => parseTargetTerm(obj)
        case unknown => throw BadRequestException(s"Expected a JSON object or string in the target array, got: $unknown")
      }
      case obj: JsObject => Seq(parseTargetTerm(obj))
      case unknown => throw BadRequestException(s"Expected `target` to be a JSON array or object, got: $unknown")
    }).map {
      case (tactics, selects) => (
        tactics,
        selects.map(_.value match {
          case w if w.nonEmpty => w
          case _ => throw BadRequestException("`select` doesn't accept empty JSON strings as values")
        })
      )
    }

  private def parseTargetTerm(target: JsObject): (JsValue, Seq[JsString]) =
    (
      target.fields.getOrElse("tactics", JsArray()),
      target.fields.get("select").map {
        case string: JsString => Seq(string)
        case arr: JsArray if arr.elements.nonEmpty =>
          arr.elements.map {
            case string: JsString => string
            case _ => throw BadRequestException("`select` must only contain JSON string values")
          }
        case _ => throw BadRequestException("`select` must be a non-empty JSON array")
      }.getOrElse(throw BadRequestException("`target` must contain a field `select`"))
    )

  get("/api/deployment-requests/:id") {
    r: DeploymentRequestGet =>
      Try(r.id.toLong).toOption.flatMap(id => Await.result(deploymentRequests.findDeploymentRequestById(db, id), 2.seconds)).map {
        depReq => {
          val cls = classOf[DeploymentRequest]
          cls.getDeclaredFields.map(_.getName).flatMap(fieldName =>
            (fieldName, cls.getDeclaredMethod(fieldName).invoke(depReq)) match {
              case ("reason", "") => None
              case ("target", json: String) => Some("target" -> RawJson(json))
              case (name, value) => Some(name -> value)
            }
          ).toMap
        }
      }
  }

  post("/api/deployment-requests") {
    r: Request => {
      r.contentString.parseJson match {
        case body: JsObject =>
          val fields = body.fields
          def read(key: String) = fields.getOrElse(key, missing(key))
          def readStr(key: String, default: Option[String] = None): String = fields.get(key) match {
            case Some(string: JsString) => string.value
            case None => default.getOrElse(missing(key))
            case unknown => throw BadRequestException(s"Expected a string as $key, got: $unknown")
          }

          val target = read("target")
          parseTargetExpression(target)

          val req = DeploymentRequest(None,
            readStr("productName"), readStr("version"), target.compactPrint, readStr("reason", Some("")),
            "anonymous", new Timestamp(System.currentTimeMillis))
          Await.result(deploymentRequests.insert(db, req).map(response.created.json), 2.seconds)

        case unknown => throw BadRequestException(s"Expected a JSON object as request body, got: $unknown")
      }
    }
  }

}
