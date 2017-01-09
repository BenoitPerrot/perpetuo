package com.criteo.perpetuo

import spray.json.JsValue


package object dispatchers {
  type Tactics = JsValue
  type Select = Seq[String]
  type Target = Seq[(Tactics, Select)]
}
