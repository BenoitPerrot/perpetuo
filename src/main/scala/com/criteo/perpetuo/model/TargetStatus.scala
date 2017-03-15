package com.criteo.perpetuo.model

import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsString, JsValue, JsonFormat}


case class TargetAtomStatus(code: Status.Code, detail: String)

object Status extends Enumeration {
  type Code = Value
  type TargetMap = Map[String, TargetAtomStatus]

  // these values should be stable in short term, since they are persisted in the DB for a few hours max
  val success = Value(1)
  val deploymentFailure = Value(2)
  val serverFailure = Value(3)
  val notDone = Value(4)

  def fromString(name: String): Status.Code =
    try {
      Status.withName(name)
    } catch {
      case _: NoSuchElementException => throw new DeserializationException(s"Unknown target status `$name`")
    }

  implicit val statusJsonFormat = new JsonFormat[Status.Code] {
    def write(status: Status.Code): JsString = {
      JsString(status.toString)
    }

    def read(value: JsValue): Status.Code = value match {
      case JsString(name) => Status.fromString(name)
      case x => throw new DeserializationException("Expected a word as atomic target status, but got " + x)
    }
  }

  val targetMapJsonFormat: JsonFormat[TargetAtomStatus] = jsonFormat2(TargetAtomStatus)
}
