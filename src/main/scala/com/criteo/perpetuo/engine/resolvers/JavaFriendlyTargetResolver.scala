package com.criteo.perpetuo.engine.resolvers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.model.Version

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetResolver extends Provider[TargetResolver] {
  def get: TargetResolver = {
    val delegate = this

    new TargetResolver {
      override def toAtoms(productName: String, productVersion: Version, targetWords: Select): Map[String, Select] =
        delegate.toAtoms(productName, productVersion, targetWords.asJava).iterateAsScala.toMap
    }
  }

  protected def toAtoms(productName: String, productVersion: Version, targetWords: JavaSet[String]): JavaMap[String, JavaIterable[String]]
}
