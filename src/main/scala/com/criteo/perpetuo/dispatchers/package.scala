package com.criteo.perpetuo

import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsonFormat}


package object dispatchers {
  type Tactics = Set[JsObject]
  type Select = Set[String]
  type TargetExpr = Set[TargetTerm]

  case class TargetTerm(tactics: Tactics = Set(JsObject()), select: Select)
  implicit def targetTermJsonFormat: JsonFormat[TargetTerm] = jsonFormat2(TargetTerm)
}
