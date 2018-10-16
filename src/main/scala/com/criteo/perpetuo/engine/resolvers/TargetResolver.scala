package com.criteo.perpetuo.engine.resolvers

import com.criteo.perpetuo.dao
import com.criteo.perpetuo.engine.{Provider, TargetAtomSet, UnprocessableIntent}
import com.criteo.perpetuo.model._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


/**
  * A target name (expressing where to deploy to) can cover more precise targets (e.g. the
  * European Union as a location includes all its member states, each of which may or may
  * not make sense as a target for a deployment); a more complex target expression can also
  * be used to express where to deploy to (i.e. deploy here but not there).
  * Resolving a target consists in computing the list of all the underlying targets covered
  * by an expression (which can be as simple as a name).
  * An atomic target is a target that cannot be resolved to smaller parts of itself because
  * FOR THE GIVEN PRODUCT, it will NEVER be possible to deploy to a subset of that target.
  * If the user can choose among "France", "Paris" and "Lyon" for a target and nothing else,
  * then "Paris" and "Lyon" ARE atoms, and "France" IS NOT. "Paris" and "Lyon" both have the
  * tag "France".
  * An atom in a user input is resolved to itself.
  * A tag in a user input is resolved to the union of all the atoms that have that tag, so
  * "France" is resolved as "Paris ∪ Lyon".
  *
  * In order to support MORE than combinations of atoms in the incoming target expressions,
  * e.g. tags, at least one of the following methods must be overridden:
  * - `getAllAtomsAndTags` to get the entire `TargetGrammar` supported out of the box
  * - `resolveNonAtoms` to provide a custom resolver, possibly allowing a custom grammar
  **/
trait TargetResolver extends Provider[TargetResolver] {
  def get: TargetResolver = this

  /**
    * Return ALL the available atoms and their tags. If the returned map is empty, only atoms
    * are supported in the user intended target.
    */
  def getAllAtomsAndTags(productName: String): Future[TargetSet] = Future.successful(TargetSet(Map.empty))

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
    * Resolve non-atoms with respect to the given product and version.
    *
    * If a target term does not exist or is not applicable to the given product, associate the
    * empty list to this term in the result or directly raise an UnprocessableIntent (for a
    * custom error message).
    *
    * @return the set of target atoms mapped to each target term thus resolved
    */
  // todo: make it asynchronous
  protected def resolveNonAtoms(productName: String, productVersion: Version, targetTerms: Set[TargetNonAtom]): (Map[TargetNonAtom, Set[TargetAtom]], Boolean) = {
    val TargetSet(atomsToTags, isExact) = Await.result(getAllAtomsAndTags(productName), Duration.Inf)

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
  * (otherwise, it's only guaranteed to be a SUPERSET of targets as a best effort for the end user
  * but the executor must take care of filtering the relevant target atoms).
  */
case class TargetSet(atomsToTags: Map[String, Seq[String]], isExact: Boolean = false)
