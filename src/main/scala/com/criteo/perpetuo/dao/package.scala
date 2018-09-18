package com.criteo.perpetuo

import com.criteo.perpetuo.model.{TargetAtom, Version}
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.language.implicitConversions


package object dao {

  type DBIOrw[T] = DBIOAction[T, NoStream, Effect.Read with Effect.Write]

  implicit class UserName(val input: String) extends AnyVal with StringInput {
    override def maxLength = 64

    override def isTruncatable = true
  }

  implicit class Comment(val input: String) extends AnyVal with StringInput {
    override def maxLength = 4000

    override def isTruncatable = true
  }

  implicit class TargetAtomField(val input: String) extends AnyVal with StringInput {
    override def maxLength = 128

    def toModel: TargetAtom = TargetAtom(input)
  }

  implicit def fromModelToDao(atom: TargetAtom): TargetAtomField = TargetAtomField(atom.name)

  val targetAtomMaxLength: Int = maxLength[TargetAtomField]

  implicit class VersionField(val input: String) extends AnyVal with StringInput {
    override def maxLength: Int = 1024

    def toModel: Version = Version(input)
  }

  implicit def fromModelToDao(v: Version): VersionField = VersionField(v.serialized)

  def maxLength[T <: AnyVal with StringInput](implicit cast: String => T): Int =
    cast("").maxLength // since T is an AnyVal, there is no instantiation

}
