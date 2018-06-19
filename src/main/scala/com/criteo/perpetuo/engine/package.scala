package com.criteo.perpetuo

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model._
import slick.dbio.{DBIOAction, Effect, NoStream}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsonFormat}

import scala.collection.JavaConverters._


package object engine {
  type Tactics = Set[JsObject]
  type Select = Set[String]
  type TargetExpr = Set[TargetTerm]

  case class TargetTerm(tactics: Tactics = Set(JsObject()), select: Select)
  implicit def targetTermJsonFormat: JsonFormat[TargetTerm] = jsonFormat2(TargetTerm)

  implicit class ExprToSelect(targetExpr: TargetExpr) {
    def select: Select = targetExpr.flatMap(_.select)
  }


  implicit class TargetGroups[T](javaTargetGroups: JavaMap[T, JavaIterable[String]]) {
    def iterateAsScala: Iterator[(T, Select)] =
      javaTargetGroups.entrySet.iterator.asScala
        .map(entry => entry.getKey -> entry.getValue.iterator.asScala.toSet)
  }


  trait Provider[T] {
    def get: T
  }


  type DBIOrw[T] = DBIOAction[T, NoStream, Effect.Read with Effect.Write]
  type ExecutionsToTrigger = Iterable[(Long, Version, TargetExpr, ExecutionTrigger)]
  type OperationCreationParams = (Operation.Kind, Iterable[(ExecutionSpecification, Vector[(ExecutionTrigger, TargetExpr)])], Option[Select])
}
