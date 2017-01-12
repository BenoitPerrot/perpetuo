package com.criteo.perpetuo

import spray.json.JsObject


package object dispatchers {
  type Tactics = Seq[JsObject]
  type Select = Seq[String]
  type Target = Seq[(Tactics, Select)]
}
