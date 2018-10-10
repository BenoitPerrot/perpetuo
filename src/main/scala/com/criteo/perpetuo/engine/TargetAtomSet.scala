package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.TargetAtom

final case class TargetAtomSet(items: Set[TargetAtom], isExact: Boolean = true) {
  // - isExact = true <=> exact set of atoms, directly usable everywhere
  // - otherwise <=> superset but may contain a "*", so only usable for dispatching (must be considered unresolved elsewhere)

  def union(x: TargetAtomSet): TargetAtomSet = {
    if (isExact != x.isExact) // fixme: do better than that if something like isExact remains
      throw new RuntimeException("Cannot merge two targets of two different resolution levels") // will never occur because they come from the same product
    TargetAtomSet(items.union(x.items), isExact)
  }
}
