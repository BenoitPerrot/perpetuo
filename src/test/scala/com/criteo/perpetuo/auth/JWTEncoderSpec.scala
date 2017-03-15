package com.criteo.perpetuo.auth

import java.nio.charset.StandardCharsets

import com.twitter.inject.Test

class JWTEncoderSpec extends Test {

  "JWTEncoder" should {

    val secret = "Some Not-so-secret String".getBytes(StandardCharsets.UTF_8)
    val payload = """{ "foo": "bar", "x": false }"""
    val token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAiZm9vIjogImJhciIsICJ4IjogZmFsc2UgfQ.T_gZzmuJ3NgZ12zakuhdq-nqAHm2wXXYwPN8UDzXG9E"

    "encode payload" in {
      val code = new JWTEncoder(secret)
      code.encode(payload) shouldBe token
    }

    "decode payload" in {
      val code = new JWTEncoder(secret)
      code.decode(token).get shouldBe payload
    }
  }
}
