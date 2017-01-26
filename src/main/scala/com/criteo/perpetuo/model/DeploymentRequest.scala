package com.criteo.perpetuo.model

import java.lang.reflect.Modifier

import com.criteo.perpetuo.app.RawJson
import com.criteo.perpetuo.dispatchers.TargetExpr
import spray.json._

case class DeploymentRequest(id: Option[Long],
                             productId: Int,
                             version: String,
                             target: String,
                             reason: String, // Not an `Option` because it's easier to consider that no comment <=> empty
                             creator: String,
                             creationDate: java.sql.Timestamp) {

  // laziness of parsedTarget is handled by hand, to be able to duplicate the instance (see `setId`)
  // and still benefit from an already parsed target without forcing it
  private var parsedTargetCache: Option[TargetExpr] = None
  def parsedTarget: TargetExpr = parsedTargetCache.getOrElse {
    val parsed = DeploymentRequestParser.parseTargetExpression(target.parseJson)
    parsedTargetCache = Some(parsed)
    parsed
  }

  def copyWithId(actualId: Long): DeploymentRequest = {
    require(id.isEmpty)
    val clone = copy(id = Some(actualId))
    clone.parsedTargetCache = parsedTargetCache
    clone
  }

  // TODO: Dedicated, actual data model is required
  def toJsonReadyMap(product: Product): Map[String, AnyRef] = {
    val cls = classOf[DeploymentRequest]
    cls.getDeclaredFields
      .filterNot(_.isSynthetic)
      .map(_.getName)
      .map(cls.getDeclaredMethod(_))
      .filterNot(method => Modifier.isPrivate(method.getModifiers))
      .flatMap(method =>
        (method.getName, method.invoke(this)) match {
          case ("reason", "") => None
          case ("productId", _) => Some("productName" -> product.name)
          case ("target", json: String) => Some("target" -> RawJson(json))
          case (name, value) => Some(name -> value)
        }
      ).toMap
  }
}