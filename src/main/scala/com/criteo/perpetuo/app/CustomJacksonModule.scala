package com.criteo.perpetuo.app

import com.criteo.perpetuo.model.{TargetAtom, Version}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{PropertyNamingStrategy, SerializerProvider}
import com.twitter.finatra.json.modules.FinatraJacksonModule
import spray.json.JsValue


object CustomJacksonModule extends FinatraJacksonModule {
  override protected val propertyNamingStrategy: PropertyNamingStrategy =
    PropertyNamingStrategy.LOWER_CAMEL_CASE

  override val additionalJacksonModules = Seq(
    new SimpleModule {
      addSerializer(JsValueSerializer)
      addSerializer(RawJsonSerializer)
      addSerializer(TimestampSerializer)
      addSerializer(TargetAtomSerializer)
      addSerializer(VersionSerializer)
      addSerializer(EnumSerializer)
    }
  )
}


case class RawJson(raw: String)


object JsValueSerializer extends StdSerializer[JsValue](classOf[JsValue]) {
  def serialize(value: JsValue, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeRawValue(value.compactPrint)
  }
}


object RawJsonSerializer extends StdSerializer[RawJson](classOf[RawJson]) {
  def serialize(json: RawJson, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeRawValue(json.raw)
  }
}


object TimestampSerializer extends StdSerializer[java.sql.Timestamp](classOf[java.sql.Timestamp]) {
  def serialize(timestamp: java.sql.Timestamp, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeNumber(timestamp.getTime / 1000)
  }
}


object TargetAtomSerializer extends StdSerializer[TargetAtom](classOf[TargetAtom]) {
  def serialize(atom: TargetAtom, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeString(atom.name)
  }
}


object VersionSerializer extends StdSerializer[Version](classOf[Version]) {
  def serialize(version: Version, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeRawValue(version.toString)
  }
}


object EnumSerializer extends StdSerializer[Enumeration#Value](classOf[Enumeration#Value]) {
  def serialize(value: Enumeration#Value, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
    jgen.writeString(value.toString)
  }
}
