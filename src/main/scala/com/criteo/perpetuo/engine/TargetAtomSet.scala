package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.TargetAtom

import scala.collection.{GenSet, GenTraversableOnce}


/**
  * @param subset      all targets that are expected to be deployed, either because they are explicitly asked by the
  *                    user or because it's covered by the user intent and we know it's eligible for actual deployment
  * @param alsoCovered targets that are technically covered by the user intent but which might not make sense for the
  *                    deployed product: it's up to each executor to decide whether it will deploy them too.
  *
  *                    Note that the two sets are not necessarily disjoint, the latter may or may not include targets
  *                    from the first (whether it does or not has no consequence).
  */
final class TargetAtomSet(val subset: Set[TargetAtom], alsoCovered: Iterable[TargetAtom]) {
  val superset: Set[TargetAtom] = subset ++ alsoCovered

  def union(rhs: TargetAtomSet): TargetAtomSet =
    TargetAtomSet(subset.union(rhs.subset), superset ++ rhs.superset)

  def intersect(rhs: TargetAtomSet): TargetAtomSet =
    &(rhs.superset)

  def &(atoms: GenSet[TargetAtom]): TargetAtomSet =
    TargetAtomSet(subset & atoms, superset & atoms)

  def --(atoms: GenTraversableOnce[TargetAtom]): TargetAtomSet =
    TargetAtomSet(subset -- atoms, superset -- atoms)
}


object TargetAtomSet {
  val empty = apply(Set.empty)

  def apply(impacted: Set[TargetAtom], alsoCovered: Iterable[TargetAtom] = Iterable.empty): TargetAtomSet =
    new TargetAtomSet(impacted, alsoCovered)
}
