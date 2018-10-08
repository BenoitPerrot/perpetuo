package com.criteo.perpetuo.model


sealed trait TargetExpr extends Any {
  def nonEmpty: Boolean

  final def isEmpty: Boolean = !nonEmpty
}


sealed trait TargetTerm extends Any with TargetExpr {
  override def nonEmpty: Boolean = true
}

final case class TargetAtom(name: String) extends AnyVal with TargetTerm {
  override def toString: String = name
}

sealed trait TargetNonAtom extends Any with TargetTerm

final case class TargetTag(tag: String) extends AnyVal with TargetNonAtom {
  override def toString: String = s"tag:$tag"
}

final case class TargetIcontains(sub: String) extends AnyVal with TargetNonAtom { // i.e. the target insensitively contains "xxx"
  override def toString: String = s"~$sub"
}

object TargetTop extends TargetNonAtom {
  override def toString: String = "*"
}

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

final case class TargetIntersection(items: Set[TargetExpr]) extends TargetOp {
  val sep = " ∩ "

  override def toString: String = if (items.nonEmpty) super.toString else TargetTop.toString
}

final case class TargetUnion(items: Set[TargetExpr]) extends TargetOp {
  val sep = " ∪ "
}

final case class TargetAtomSet(items: Set[TargetAtom], isExact: Boolean = true) extends TargetOp {
  // - isExact = true <=> exact set of atoms, directly usable everywhere
  // - otherwise <=> superset but may contain a "*", so only usable for dispatching (must be considered unresolved elsewhere)

  val sep = ", "

  def union(x: TargetAtomSet) = {
    if (isExact != x.isEmpty) // fixme: do better than that if something like isExact remains
      throw new RuntimeException("Cannot merge two targets of two different resolution levels") // will never occur because they come from the same product
    TargetAtomSet(items.union(x.items), isExact)
  }
}
