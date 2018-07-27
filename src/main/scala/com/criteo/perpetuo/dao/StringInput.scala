package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.UnprocessableIntent
import slick.lifted.MappedTo


trait StringInput extends Any with MappedTo[String] {
  val input: String

  def maxLength: Int

  def isTruncatable: Boolean = false

  override def toString: String = input

  override def value: String =
    if (maxLength < input.length) {
      if (!isTruncatable) {
        val hint = if (100 < input.length) s"${input.substring(0, 80)}...${input.substring(input.length - 10)}" else input
        throw UnprocessableIntent(s"Too long input (${input.length}-char. long for max. $maxLength): $hint")
      }
      input.substring(0, maxLength - 3) + "..."
    }
    else
      input
}
