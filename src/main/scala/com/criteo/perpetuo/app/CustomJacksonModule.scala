package com.criteo.perpetuo.app

import com.criteo.perpetuo.model.Version
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.finatra.json.utils.CamelCasePropertyNamingStrategy


object CustomJacksonModule extends FinatraJacksonModule {
  override val additionalJacksonModules = Seq(
    new SimpleModule {
      addSerializer(RawJsonSerializer)
      addSerializer(TimestampSerializer)
      addSerializer(VersionSerializer)
      addSerializer(EnumSerializer)
    }
  )

  override val propertyNamingStrategy = CamelCasePropertyNamingStrategy
}


case class RawJson(raw: String)


object RawJsonSerializer extends StdSerializer[RawJson](classOf[RawJson]) {
  def serialize(json: RawJson, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeRawValue(json.raw)
  }
}


object TimestampSerializer extends StdSerializer[java.sql.Timestamp](classOf[java.sql.Timestamp]) {
  def serialize(timestamp: java.sql.Timestamp, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeNumber(timestamp.getTime / 1000)
  }
}


object VersionSerializer extends StdSerializer[Version](classOf[Version]) {
  def serialize(version: Version, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeString(version.toString)
  }
}


object EnumSerializer extends StdSerializer[Enumeration#Value](classOf[Enumeration#Value]) {
  def serialize(value: Enumeration#Value, jgen: JsonGenerator, provider: SerializerProvider) {
    jgen.writeString(value.toString)
  }
}
