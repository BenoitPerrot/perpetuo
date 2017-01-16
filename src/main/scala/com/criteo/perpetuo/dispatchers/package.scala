package com.criteo.perpetuo

import spray.json.JsObject


package object dispatchers {
  type Tactics = Iterable[JsObject]
  type Select = Iterable[String]
  type TargetExpr = Iterable[TargetTerm]

  case class TargetTerm(tactics: Tactics = Seq(JsObject()), select: Select)
}
