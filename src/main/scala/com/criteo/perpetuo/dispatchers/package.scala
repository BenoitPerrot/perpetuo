package com.criteo.perpetuo

import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsonFormat}


package object dispatchers {
  type Tactics = Iterable[JsObject]
  type Select = Iterable[String]
  type TargetExpr = Iterable[TargetTerm]

  case class TargetTerm(tactics: Tactics = Seq(JsObject()), select: Select)
  implicit def targetTermJsonFormat: JsonFormat[TargetTerm] = jsonFormat2(TargetTerm)
}
