package com.criteo.perpetuo.model

import com.criteo.perpetuo.app.RawJson
import com.criteo.perpetuo.engine.TargetExpr
import spray.json._

import scala.collection.JavaConverters._


trait ParsedTarget {
  val target: String

  // laziness of parsedTarget is handled by hand, to be able to duplicate the instance
  // and still benefit from an already parsed target without forcing it
  private[model] var parsedTargetCache: Option[TargetExpr] = None

  def parsedTarget: TargetExpr = parsedTargetCache.getOrElse {
    val parsed = DeploymentRequestParser.parseTargetExpression(target.parseJson)
    parsedTargetCache = Some(parsed)
    parsed
  }
}

object Target {
  def getSimpleSelect(target: TargetExpr): Iterable[String] = target.flatMap(_.select)

  def getSimpleSelectForGroovy(target: TargetExpr): java.lang.Iterable[String] = target.flatMap(_.select).toIterable.asJava
}


trait DeploymentRequest extends ParsedTarget {
  val id: Long
  val productId: Int
  val version: Version
  val target: String
  val comment: String
  val creator: String
  val creationDate: java.sql.Timestamp
}


class DeploymentRequestAttrs(val productName: String,
                             val version: Version,
                             val plan: Seq[ProtoDeploymentPlanStep],
                             val comment: String,
                             val creator: String,
                             val creationDate: java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis)) extends ParsedTarget {
  val target: String = {
    assert(plan.size == 1) // TODO: remove once migration complete
    plan.head.targetExpression.compactPrint
  }
}


case class ShallowDeploymentRequest(id: Long,
                                    productId: Int,
                                    version: Version,
                                    target: String,
                                    comment: String,
                                    creator: String,
                                    creationDate: java.sql.Timestamp) extends DeploymentRequest


case class DeepDeploymentRequest(id: Long,
                                 product: Product,
                                 version: Version,
                                 target: String,
                                 comment: String,
                                 creator: String,
                                 creationDate: java.sql.Timestamp) extends DeploymentRequest {

  val productId: Int = product.id

  def copyParsedTargetCacheFrom(obj: ParsedTarget): Unit = {
    parsedTargetCache = obj.parsedTargetCache
  }

  def toJsonReadyMap: Map[String, Any] = {
    val cls = classOf[DeepDeploymentRequest]
    cls.getDeclaredFields.map(_.getName).toIterator.zip(productIterator).flatMap({
      case ("comment", "") => None
      case (_, product: Product) => Some("productName" -> product.name)
      case ("target", json: String) => Some("target" -> RawJson(json))
      case (name, value) => Some(name -> value)
    }).toMap
  }
}
