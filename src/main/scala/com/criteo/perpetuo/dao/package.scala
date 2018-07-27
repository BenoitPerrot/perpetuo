package com.criteo.perpetuo

import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.language.implicitConversions


package object dao {

  type DBIOrw[T] = DBIOAction[T, NoStream, Effect.Read with Effect.Write]

  implicit class UserName(val input: String) extends AnyVal with StringInput {
    override def maxLength = 64

    override def isTruncatable = true
  }

  def maxLength[T <: AnyVal with StringInput](implicit cast: String => T): Int =
    cast("").maxLength // since T is an AnyVal, there is no instantiation

}
