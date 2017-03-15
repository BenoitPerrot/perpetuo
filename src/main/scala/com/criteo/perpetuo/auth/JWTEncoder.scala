package com.criteo.perpetuo.auth

import java.nio.charset.StandardCharsets
import javax.crypto.{Mac, spec}

import com.twitter.util.Base64UrlSafeStringEncoder

/**
  * Provides encoding & decoding of JSON Web Token, using the HS256 scheme
  * See http://jwt.io for more info.
  */
class JWTEncoder(private val secret : Array[Byte]) {

  private val HMAC_SHA256 = "HmacSHA256"

  private val secretSpec = new spec.SecretKeySpec(secret, HMAC_SHA256)
  private val header = """{"alg":"HS256","typ":"JWT"}"""
  private val headerInBase64 = Base64UrlSafeStringEncoder.encode(header.getBytes)

  private def signInBase64(unsignedToken: String): String = {
    val code = Mac.getInstance(HMAC_SHA256)
    code.init(secretSpec)
    Base64UrlSafeStringEncoder.encode(code.doFinal(unsignedToken.getBytes(StandardCharsets.UTF_8)))
  }

  def encode(payload: String): String = {
    val unsignedToken = headerInBase64 + "." + Base64UrlSafeStringEncoder.encode(payload.getBytes)
    unsignedToken + "." + signInBase64(unsignedToken)
  }

  def decode(jwt: String): Option[String] = {
    val parts = jwt.split('.')
    if (parts.length == 3) {
      if (headerInBase64 == parts(0)) {
        val payloadInBase64 = parts(1)
        val signatureInBase64 = parts(2)
        val unsignedToken = headerInBase64 + "." + payloadInBase64
        if (signatureInBase64 == signInBase64(unsignedToken)) {
          return Some(new String(Base64UrlSafeStringEncoder.decode(payloadInBase64), StandardCharsets.UTF_8))
        }
      }
    }
    None
  }

}
