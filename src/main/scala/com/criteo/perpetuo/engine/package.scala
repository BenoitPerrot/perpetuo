package com.criteo.perpetuo

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import com.criteo.perpetuo.engine.invokers.ExecutorInvoker
import com.criteo.perpetuo.model.{ShallowOperationTrace, Version}
import slick.dbio.{DBIOAction, Effect, NoStream}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsonFormat}

import scala.collection.JavaConverters._
import scala.concurrent.Future


package object engine {
  type Tactics = Set[JsObject]
  type Select = Set[String]
  type TargetExpr = Set[TargetTerm]

  case class TargetTerm(tactics: Tactics = Set(JsObject()), select: Select)
  implicit def targetTermJsonFormat: JsonFormat[TargetTerm] = jsonFormat2(TargetTerm)

  implicit class ExprToSelect(targetExpr: TargetExpr) {
    def select: Set[String] = targetExpr.flatMap(_.select)
  }


  implicit class TargetGroups[T](javaTargetGroups: JavaMap[T, JavaIterable[String]]) {
    def iterateAsScala: Iterator[(T, Set[String])] =
      javaTargetGroups.entrySet.iterator.asScala
        .map(entry => entry.getKey -> entry.getValue.iterator.asScala.toSet)
  }


  trait Provider[T] {
    def get: T
  }


  type ExecutionsToTrigger = Iterable[(Long, Version, TargetExpr, ExecutorInvoker)]
  type OperationStartSpecifics = Future[(DBIOAction[(ShallowOperationTrace, ExecutionsToTrigger), NoStream, Effect.Read with Effect.Write], Option[Set[String]])]
}
