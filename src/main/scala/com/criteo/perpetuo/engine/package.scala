package com.criteo.perpetuo

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model._

import scala.collection.JavaConverters._


package object engine {
  type TargetExpr = Set[String]


  implicit class TargetGroups[A, B](javaTargetGroups: JavaMap[A, JavaIterable[B]]) {
    def iterateAsScala: Iterator[(A, Set[B])] =
      javaTargetGroups.entrySet.iterator.asScala
        .map(entry => entry.getKey -> entry.getValue.iterator.asScala.toSet)
  }


  trait Provider[T] {
    def get: T
  }


  type ExecutionsToTrigger = Iterable[(Long, Version, TargetExpr, ExecutionTrigger)]
  type OperationCreationParams = (Operation.Kind, Iterable[(ExecutionSpecification, Vector[(ExecutionTrigger, TargetExpr)])], Option[Set[TargetAtom]])
}
