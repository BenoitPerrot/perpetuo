package com.criteo.perpetuo.engine.resolvers

import java.lang.{Iterable => JavaIterable}

import com.criteo.perpetuo.engine.Provider

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetResolver extends Provider[TargetResolver] {
  def get: TargetResolver = {
    val delegate = this

    new TargetResolver {
      override def toAtoms(productName: String, productVersion: String, targetWord: String): Iterable[String] =
        delegate.toAtoms(productName, productVersion, targetWord).asScala
    }
  }

  protected def toAtoms(productName: String, productVersion: String, targetWord: String): JavaIterable[String]
}



