package com.criteo.perpetuo.util.json

import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * This class eases the invocation of spray "toJson" from Groovy scripts,
  * where it is a pain to invoke implicit methods
  */
object ToJsonAlias {
  def deepToJson(m: Map[_, _]): JsObject =
    m.asInstanceOf[Map[String, _]].mapValues(deepToJson).toJson.asInstanceOf[JsObject]

  def deepToJson(a: Seq[_]): JsArray =
    a.map(deepToJson).toJson.asInstanceOf[JsArray]

  def deepToJson(v: Any): JsValue = v match {
    case m: Map[_, _] => deepToJson(m)
    case a: Seq[_] => deepToJson(a)
    case s: String => s.toJson
    case n: BigDecimal => n.toJson
    case i: Long => i.toJson
    case i: Int => i.toJson
    case b: Boolean => b.toJson
    case j: JsValue => j
  }
}
