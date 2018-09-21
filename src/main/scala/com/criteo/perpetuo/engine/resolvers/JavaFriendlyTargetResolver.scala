package com.criteo.perpetuo.engine.resolvers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.model.{TargetAtom, TargetExpr, Version}
import com.twitter.inject.Logging

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetResolver extends Provider[TargetResolver] with Logging {
  def get: TargetResolver = {
    val delegate = this

    new TargetResolver {
      override def toAtoms(productName: String, productVersion: Version, targetExpr: TargetExpr): Option[Map[String, Set[TargetAtom]]] =
        Option(delegate.toAtoms(productName, productVersion, targetExpr.asJava)).map(_.iterateAsScala.toMap)
    }
  }

  /**
    * A method easier to implement in Java or Groovy than com.criteo.perpetuo.engine.resolvers.TargetResolver.toAtoms;
    * the difference is in the types used: for instance, it must return null where the documentation mentions None.
    */
  protected def toAtoms(productName: String, productVersion: Version, targetWords: JavaSet[String]): JavaMap[String, JavaIterable[TargetAtom]]
}
