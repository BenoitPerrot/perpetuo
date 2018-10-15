package com.criteo.perpetuo.engine.resolvers

import com.criteo.perpetuo.dao
import com.criteo.perpetuo.engine.{Provider, TargetAtomSet, UnprocessableIntent}
import com.criteo.perpetuo.model._
import com.twitter.util.{Await, Future}


trait TargetResolver extends Provider[TargetResolver] {
  def get: TargetResolver = this

  def getAllAtomsAndTags(productName: String): Future[TargetSet] = Future.value(TargetSet(Map.empty))

  def resolveExpression(productName: String, productVersion: Version, targetExpr: TargetExpr): TargetAtomSet = {
    // resolve what's unresolved
    val nonAtoms = extractNonAtoms(targetExpr)
    val (nonAtomsToAtoms, isExact) = resolveNonAtoms(productName, productVersion, nonAtoms)

    // check resolution's consistency
    val resolvedWords = nonAtomsToAtoms.keySet
    if (!resolvedWords.subsetOf(nonAtoms))
      throw new RuntimeException("The resolver augmented the original intent, which is forbidden. The targets introduced by the resolver are: " +
        (resolvedWords -- nonAtoms).mkString(", "))

    val emptyWords = nonAtomsToAtoms.flatMap {
      case (_, atoms) if atoms.nonEmpty =>
        atoms.foreach(atom => assert(atom.name.length <= dao.targetAtomMaxLength, s"Target `$atom` is too long"))
        None
      case (word, _) =>
        Some(word)
    }
    if (emptyWords.nonEmpty || resolvedWords.size != nonAtoms.size)
      throw UnprocessableIntent("The following target(s) were not resolved: " +
        (emptyWords.iterator ++ (nonAtoms -- resolvedWords)).mkString(", "))

    // transform the expression to atoms
    val atomSet = transform(targetExpr, nonAtomsToAtoms)
    if (isExact)
      TargetAtomSet(atomSet.superset)
    else
      atomSet
  }

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
  // todo: make it asynchronous
  protected def resolveNonAtoms(productName: String, productVersion: Version, targetTerms: Set[TargetNonAtom]): (Map[TargetNonAtom, Set[TargetAtom]], Boolean) = {
    val TargetSet(atomsToTags, isExact) = Await.result(getAllAtomsAndTags(productName))

    lazy val targetAtoms = atomsToTags.keySet.map(TargetAtom)
    lazy val tagsToAtoms = atomsToTags
      .toStream
      .flatMap { case (atom, tags) =>
        tags.iterator.map(_ -> atom)
      }
      .groupBy { case (tag, _) => tag }
      .map { case (tag, tagsAtoms) =>
        tag -> tagsAtoms.map { case (_, atom) => TargetAtom(atom) }
      }

    val resolved = targetTerms.map {
      case tag@TargetTag(name) =>
        tag -> tagsToAtoms.getOrElse(name, throw UnprocessableIntent(s"Unknown tag `$name` in target expression")).toSet
      case icontains@TargetIcontains(sub) =>
        icontains -> targetAtoms.filter(_.name.contains(sub))
      case TargetTop =>
        TargetTop -> targetAtoms
    }.toMap

    (resolved, isExact)
  }

  private val extractNonAtoms: TargetExpr => Set[TargetNonAtom] = {
    case n: TargetNonAtom => Set(n)
    case i: TargetIntersection if i.isEmpty => Set(TargetTop)
    case op: TargetOp => op.items.flatMap(extractNonAtoms).toSet
    case _: TargetAtom => Set.empty
  }

  private def transform(targetExpr: TargetExpr, f: TargetNonAtom => Set[TargetAtom]): TargetAtomSet = targetExpr match {
    case n: TargetNonAtom => TargetAtomSet(Set.empty, f(n))
    case a: TargetAtom => TargetAtomSet(Set(a))
    case TargetIntersection(items) => items
      .map(transform(_, f))
      .reduceOption(_ intersect _)
      .getOrElse(TargetAtomSet(Set.empty, f(TargetTop)))
    case TargetUnion(items) => items
      .map(transform(_, f))
      .reduceOption(_ union _)
      .getOrElse(TargetAtomSet.empty)
  }
}

/**
  * Return type of [[TargetResolver.getAllAtomsAndTags]]: the map from atoms to tags and a boolean
  * which is true if and only if it's certain that all atoms are relevant for the product
  * (otherwise, it is a *superset* of targets as a best effort for the end user but the executor
  * must take care of filtering the relevant target atoms).
  */
case class TargetSet(atomsToTags: Map[String, Seq[String]], isExact: Boolean = false)
