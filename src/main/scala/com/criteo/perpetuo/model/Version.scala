package com.criteo.perpetuo.model

import slick.lifted.MappedTo

class Version(input: String) extends MappedTo[String] {
  // compute the standardized representation, usable by Slick's `sortBy` thanks to `MappedTo`, and by `equals`
  val value: String = Version.numberRegex.replaceAllIn(input, { m =>
    val nb = m.matched
    val prefix = Version.numberBaseField.length - nb.length
    if (prefix < 0)
      throw new IllegalArgumentException("Too long number: " + nb)
    Version.numberBaseField.slice(0, prefix) + nb
  })

  override def toString: String = Version.dropLeading0Regex.replaceAllIn(value, _.group(1))

  override def equals(o: scala.Any): Boolean = o.isInstanceOf[Version] && o.asInstanceOf[Version].value == value
}

object Version {
  private val numberBaseField = "0" * System.currentTimeMillis.toString.length
  private val numberRegex = """\d+""".r
  private val dropLeading0Regex = """0*(\d+)""".r

  def apply(input: String): Version = new Version(input)
}
