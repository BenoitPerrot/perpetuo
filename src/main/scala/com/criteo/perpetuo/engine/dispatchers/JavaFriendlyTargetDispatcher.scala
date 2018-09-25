package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer with Logging {
  def get: TargetDispatcher = {
    val delegate = this

    new TargetDispatcher {
      override def freezeParameters(productName: String, version: Version): String =
        delegate.freezeParameters(productName, version)

      // temporary conversion but it doesn't make sense to keep this layer with a structured expression
      protected override def dispatch(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] = {
        val normalized = Dispatch.normalizeExpr(targetExpr)
        val toTerm = normalized.map(term => term.toString -> term).toMap
        delegate.dispatch(normalized.map(_.toString).asJava, frozenParameters)
          .iterateAsScala
          .toIterable
          .map { case (et, te) => (et, TargetUnion(te.map(toTerm))) }
      }
    }
  }

  protected def dispatch(targetExpr: JavaSet[String], frozenParameters: String): JavaMap[ExecutionTrigger, JavaIterable[String]]
}
