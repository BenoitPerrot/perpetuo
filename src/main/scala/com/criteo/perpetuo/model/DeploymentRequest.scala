package com.criteo.perpetuo.model

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

  def getSimpleSelectForGroovy(target: TargetExpr): java.util.Set[String] = target.flatMap(_.select).asJava
}


case class ProtoDeploymentRequest(productName: String,
                                  version: Version,
                                  plan: Seq[ProtoDeploymentPlanStep],
                                  comment: String,
                                  creator: String,
                                  creationDate: java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis)) extends ParsedTarget {
  val target: String = plan.head.targetExpression.compactPrint
}


case class DeploymentRequest(id: Long,
                             product: Product,
                             version: Version,
                             comment: String,
                             creator: String,
                             creationDate: java.sql.Timestamp) {

  val productId: Int = product.id
}
