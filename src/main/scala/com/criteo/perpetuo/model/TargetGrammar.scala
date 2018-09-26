package com.criteo.perpetuo.model


sealed trait TargetExpr extends Any {
  def nonEmpty: Boolean
}


sealed trait TargetTerm extends Any with TargetExpr {
  override def nonEmpty: Boolean = true
}

final case class TargetAtom(name: String) extends AnyVal with TargetTerm {
  override def toString: String = name
}

sealed trait TargetNonAtom extends Any with TargetTerm

// temporarily, while migrating toward typed expressions:
final case class TargetWord(word: String) extends AnyVal with TargetNonAtom {
  override def toString: String = word
}


sealed trait TargetOp extends TargetExpr {
  val items: Iterable[TargetExpr]
  val sep: String

  override def toString: String = s"""(${items.mkString(sep)})"""

  override def nonEmpty: Boolean = items.exists(_.nonEmpty)
}

final case class TargetUnion(items: Set[TargetExpr]) extends TargetOp {
  val sep = " ∪ "
}

final case class TargetAtomSet(items: Set[TargetAtom]) extends TargetOp {
  val sep = ", "

  def union(x: TargetAtomSet) = TargetAtomSet(items.union(x.items))
}
