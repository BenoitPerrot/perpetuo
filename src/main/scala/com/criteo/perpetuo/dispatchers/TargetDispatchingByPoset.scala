package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker

import scala.collection.mutable


class TargetDispatchingByPoset(val executorMap: Map[String, ExecutorInvoker],
                               val getParents: (String) => Iterable[String])
  extends TargetDispatching {
  // todo? support multiple executors per target, possibly with the choice between AnyOf[ExecutorType] and AllOf[ExecutorType]

  /**
    * the actual dispatcher
    */
  override def dispatch(select: Select): Iterator[(ExecutorInvoker, Select)] = {
    (for {
      word <- select
      executor <- getExecutors(Some(word))
    } yield (word, executor))
      .groupBy(_._2)
      .toIterator // very important to not let the keys in the map below override each other
      .map({ case (executor, words) => (executor, words.map(_._1)) })
  }


  /**
    * everything below is the mechanism to build the upper POSet
    */

  private val upperGraph: (Option[String]) => Option[Set[ExecutorInvoker]] = buildUpperGraph

  private def buildUpperGraph: (Option[String]) => Option[Set[ExecutorInvoker]] = {
    val graph = mutable.Map[Option[String], Set[ExecutorInvoker]]()
    executorMap.foreach { case (targetWord, executor) => addInGraph(graph, Some(targetWord), Set(executor)) }
    graph.get
  }

  private def addInGraph(graph: mutable.Map[Option[String], Set[ExecutorInvoker]],
                         targetAtom: Option[String],
                         executors: Set[ExecutorInvoker],
                         currentCycle: Set[Option[String]] = Set()): Unit = {
    assert(!currentCycle.contains(targetAtom),
      "The POSet doesn't have a strict order, there is a cycle in:\n" + currentCycle.mkString(", "))
    lazy val updatedCycle = currentCycle + targetAtom

    val updatedExecutors = graph.get(targetAtom).map(_ ++ executors).getOrElse(executors)
    graph(targetAtom) = updatedExecutors

    targetAtom.foreach(
      atom => for (parentAtom <- getParentsOrRoot(atom))
        if (!parentAtom.exists(executorMap.contains))
          addInGraph(graph, parentAtom, updatedExecutors, updatedCycle)
    )
  }

  private def getParentsOrRoot(targetWord: String): Iterable[Option[String]] = {
    val declaredParents = getParents(targetWord)
    if (declaredParents.nonEmpty)
      declaredParents.map(Some.apply) else
      Seq(None)
  }

  private def getExecutors(targetWord: Option[String],
                           cache: mutable.Map[String, Set[ExecutorInvoker]] = mutable.Map()): Set[ExecutorInvoker] = {
    upperGraph(targetWord).getOrElse {
      val word = targetWord.get
      cache.getOrElse(word, {
        cache(word) = Set()
        // we kindly tolerate and handle cycles at runtime, to not crash during a request
        val res = getParentsOrRoot(word).map(getExecutors(_, cache)).reduce((x, y) => x.union(y))
        cache(word) = res
        res
      })
    }
  }
}
