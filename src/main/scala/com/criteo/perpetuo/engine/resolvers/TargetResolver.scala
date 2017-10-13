package com.criteo.perpetuo.engine.resolvers

import com.criteo.perpetuo.engine.{Provider, Select}


trait TargetResolver extends Provider[TargetResolver] {
  def get: TargetResolver = this

  /**
    * A given target word can express the union of multiple targets; resolving a target consists
    * in computing the list of all the underlying targets (e.g. the European Union
    * COULD be "resolved" in this context as the list of all the names of its member states).
    * An atomic target is a target that cannot be resolved to smaller parts of itself because
    * FOR THE GIVEN PRODUCT, IT WILL NEVER BE POSSIBLE TO DEPLOY TO A SUBSET OF THAT TARGET.
    * If you can choose between Paris and Lyon as a target in France, but nothing finer, then
    * the result of this function `toAtoms` for the input "European Union" CANNOT contain "France"
    * but MUST contain at least "Paris" and "Lyon".
    * For a given atomic target (let's say "Lyon"), this function must return the input itself as
    * only element in the list (i.e. "Lyon" resolves to Iterable("Lyon")).
    *
    * @return the atomic target words to which each input target word is resolved wrt the given product and version.
    */
  def toAtoms(productName: String, productVersion: String, targetWords: Select): Map[String, Select] =
    targetWords.iterator.map(word => word -> Set(word)).toMap
}
