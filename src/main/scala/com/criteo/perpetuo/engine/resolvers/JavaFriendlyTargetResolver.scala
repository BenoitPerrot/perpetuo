package com.criteo.perpetuo.engine.resolvers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetResolver extends Provider[TargetResolver] with Logging {
  def get: TargetResolver = {
    val delegate = this

    new TargetResolver {
      // temporary conversion but it doesn't make sense to keep this layer with a structured expression
      protected override def resolveTerms(productName: String, productVersion: Version, targetTerms: Set[TargetWord]): Option[Map[TargetWord, Set[TargetAtom]]] = {
        val toTerm = targetTerms.map(term => term.toString -> term).toMap
        Option(delegate.resolveTerms(productName, productVersion, targetTerms.map(_.toString).asJava)).map(_
          .iterateAsScala
          .map { case (term, atoms) => toTerm(term) -> atoms.map(TargetAtom) }
          .toMap
        )
      }
    }
  }

  /**
    * A method easier to implement in Java or Groovy than [[com.criteo.perpetuo.engine.resolvers.TargetResolver.resolveTerms]];
    * the difference is in the types used: for instance, it must return null where the documentation mentions None.
    */
  protected def resolveTerms(productName: String, productVersion: Version, targetTerms: JavaSet[String]): JavaMap[String, JavaIterable[String]]
}
