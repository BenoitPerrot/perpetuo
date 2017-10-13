package com.criteo.perpetuo

import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsonFormat}


package object engine {
  type Tactics = Set[JsObject]
  type Select = Set[String]
  type TargetExpr = Set[TargetTerm]

  case class TargetTerm(tactics: Tactics = Set(JsObject()), select: Select)
  implicit def targetTermJsonFormat: JsonFormat[TargetTerm] = jsonFormat2(TargetTerm)

  implicit class ExprToSelect(targetExpr: TargetExpr) {
    def select: Set[String] = targetExpr.flatMap(_.select)
  }


  trait Provider[T] {
    def get: T
  }
}
