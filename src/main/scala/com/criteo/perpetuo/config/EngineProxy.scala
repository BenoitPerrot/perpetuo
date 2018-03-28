package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.Engine
import com.criteo.perpetuo.model.DeploymentRequest
import com.google.inject.{Inject, Provider, Singleton}

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Provides a simplified, limited view of the Engine
  */
@Singleton
class EngineProxy @Inject()(engineProvider: Provider[Engine]) {

  private lazy val engine = engineProvider.get // To break the dependency cycle between Plugins and Engine

  def appendComment(deploymentRequest: DeploymentRequest, comment: String): Unit =
    Await.result(
      engine.dbBinding.updateDeploymentRequestComment(
        deploymentRequest.id,
        s"${if (deploymentRequest.comment.nonEmpty) s"${deploymentRequest.comment}\n" else ""}$comment"
      ), 2.seconds
    )

  def readComment(deploymentRequest: DeploymentRequest): String =
    Await.result(engine.dbBinding.findDeepDeploymentRequestById(deploymentRequest.id).map(_.get.comment), 2.seconds)

}
