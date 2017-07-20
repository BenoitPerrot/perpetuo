package com.criteo.perpetuo.model

import com.criteo.perpetuo.app.RawJson
import com.criteo.perpetuo.engine.dispatchers.TargetExpr
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


class DeploymentRequestAttrs(val productName: String,
                             val version: Version,
                             val target: String,
                             val comment: String,
                             val creator: String,
                             val creationDate: java.sql.Timestamp) extends ParsedTarget


case class DeploymentRequest(id: Long,
                             product: Product,
                             version: Version,
                             target: String,
                             comment: String,
                             creator: String,
                             creationDate: java.sql.Timestamp) extends ParsedTarget {

  def copyParsedTargetCacheFrom(obj: ParsedTarget): Unit = {
    parsedTargetCache = obj.parsedTargetCache
  }

  def toJsonReadyMap: Map[String, Any] = {
    val cls = classOf[DeploymentRequest]
    cls.getDeclaredFields.map(_.getName).toIterator.zip(productIterator).flatMap({
      case ("comment", "") => None
      case (_, product: Product) => Some("productName" -> product.name)
      case ("target", json: String) => Some("target" -> RawJson(json))
      case (name, value) => Some(name -> value)
    }).toMap
  }
}
