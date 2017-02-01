package com.criteo.perpetuo.model

import java.lang.reflect.Modifier

import com.criteo.perpetuo.app.RawJson

class DeploymentRequestAndProduct(deploymentRequest: DeploymentRequest, val product: Product) {

  val id: Some[Long] = Some(deploymentRequest.id.get)
  val version: String = deploymentRequest.version
  val target: String = deploymentRequest.target
  val comment: String = deploymentRequest.comment
  val creator: String = deploymentRequest.creator
  val creationDate: java.sql.Timestamp = deploymentRequest.creationDate


  def toJsonReadyMap: Map[String, AnyRef] = {
    val cls = classOf[DeploymentRequestAndProduct]
    cls.getDeclaredFields
      .filterNot(_.isSynthetic)
      .map(_.getName)
      .map(cls.getDeclaredMethod(_))
      .filterNot(method => Modifier.isPrivate(method.getModifiers))
      .flatMap(method =>
        (method.getName, method.invoke(this)) match {
          case ("comment", "") => None
          case (_, p: Product) => Some("productName" -> p.name)
          case ("target", json: String) => Some("target" -> RawJson(json))
          case (name, value) => Some(name -> value)
        }
      ).toMap
  }
}