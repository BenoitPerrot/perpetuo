package com.criteo.perpetuo.engine.resolvers

import com.criteo.perpetuo.engine.{Provider, Select, TargetExpr, TargetTerm, UnprocessableIntent}
import com.criteo.perpetuo.model.{TargetAtom, Version}


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
    * If a target does not exist or must not host the given product, return the empty list or
    * raise an UnprocessableIntent.
    *
    * @return - the atomic target words mapped to each input target word being resolved wrt the given
    *         product and version;
    *         - None if it's NOT CERTAIN that ANY TARGET can be resolved to atoms for the given product.
    */
  def toAtoms(productName: String, productVersion: Version, targetWords: Select): Option[Map[String, Select]] = None

  def resolveExpression(productName: String, productVersion: Version, target: TargetExpr): Option[TargetExpr] = {
    val select = target.select
    toAtoms(productName, productVersion, select).map { toAtoms =>
      val resolvedTerms = toAtoms.keySet
      assert(resolvedTerms.subsetOf(select), "More targets after resolution than before: " + (resolvedTerms -- select).map(_.toString).mkString(", "))
      if (resolvedTerms.size != select.size)
        throw UnprocessableIntent("Unknown target(s): " + (select -- resolvedTerms).map(_.toString).mkString(", "))
      toAtoms.foreach { case (word, atoms) =>
        if (atoms.isEmpty)
          throw UnprocessableIntent(s"`$word` is not a valid target in that context")
        atoms.foreach(atom => assert(atom.length <= TargetAtom.maxSize, s"Target `$atom` is too long"))
      }
      target.map(term => TargetTerm(term.tactics, term.select.iterator.flatMap(toAtoms).toSet))
    }
  }
}
