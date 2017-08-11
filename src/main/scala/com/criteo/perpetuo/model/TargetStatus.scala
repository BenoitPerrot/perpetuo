package com.criteo.perpetuo.model

import spray.json.DefaultJsonProtocol._
import spray.json.{DeserializationException, JsString, JsValue, JsonFormat}


case class TargetAtomStatus(code: Status.Code, detail: String) // fixme: probably to remove when DB migration is over

case class TargetStatus(id: Long,
                        executionId: Long,
                        targetAtom: String,
                        code: Status.Code,
                        detail: String)

object TargetAtom {
  type Type = String
  val maxSize = 128
}

object Status extends Enumeration {
  type Code = Value
  type TargetMap = Map[TargetAtom.Type, TargetAtomStatus] // todo (2017.05.24): remove when schema update is over

  // these values must be stable, since they are persisted in the DB
  val running = Value(0)
  val success = Value(1)
  val productFailure = Value(2)
  val hostFailure = Value(3)
  val notDone = Value(4)

  implicit val statusJsonFormat = new JsonFormat[Status.Code] {
    def write(status: Status.Code): JsString = {
      JsString(status.toString)
    }

    def read(value: JsValue): Status.Code = value match {
      case JsString(name) => try {
        Status.withName(name)
      } catch {
        case _: NoSuchElementException => throw new DeserializationException(s"Unknown target status `$name`")
      }
      case x => throw new DeserializationException("Expected a word as atomic target status, but got " + x)
    }
  }

  val targetMapJsonFormat: JsonFormat[TargetAtomStatus] = jsonFormat2(TargetAtomStatus)
}
