package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker
import groovy.lang.Closure

import scala.collection.JavaConverters._
import scala.collection.mutable


class ExecutorsByPoset(val executorMap: Map[String, ExecutorInvoker],
                       val getParents: (String) => Iterable[String]) {

  def getExecutors(selectWord: String): Set[ExecutorInvoker] = {
    upperGraph(selectWord).orElse(getOriginalExecutor(selectWord).map(Set(_))).getOrElse(
      throw new Exception(s"$selectWord is not covered by executors; can't operate.")
    )
  }

  /**
    * @return the executor associated to one of the closest originally-labeled ascendant nodes,
    *         starting from the input node itself. Note: it doesn't look at the generated upper POSet.
    */
  private def getOriginalExecutor(selectWord: String,
                                  cache: mutable.Map[String, ExecutorAndDepth] = mutable.Map()): Option[ExecutorInvoker] =
    getExecutorAndDepth(selectWord, cache).map(_._1)

  /**
    * @return the same as above, plus the related depth at which the executor has been found, when applicable.
    */
  private def getExecutorAndDepth(selectWord: String,
                                  cache: mutable.Map[String, ExecutorAndDepth],
                                  depth: Int = 0): ExecutorAndDepth = {
    executorMap.get(selectWord)
      .map(exec => Some((exec, depth)))
      .getOrElse {
        cache.getOrElse(selectWord, {
          cache(selectWord) = None // we kindly tolerate and handle cycles at runtime, to not crash during a request

          val labeled = getParents(selectWord).flatMap(getExecutorAndDepth(_, cache, depth + 1))
          if (labeled.nonEmpty) {
            val ret = Some(labeled.minBy(_._2))
            cache(selectWord) = ret
            ret
          }
          else
            None
        })
      }
  }

  private type ExecutorAndDepth = Option[(ExecutorInvoker, Int)]

  /**
    * @return the executors associated to a node that is in the upper part (not necessarily balanced)
    *         of the POSet, from all originally labeled nodes and above. If the input node is not in that
    *         case, this function returns None.
    */
  private val upperGraph: (String) => Option[Set[ExecutorInvoker]] = buildUpperGraph

  private def buildUpperGraph: (String) => Option[Set[ExecutorInvoker]] = {
    val graph = mutable.Map[String, Set[ExecutorInvoker]]()
    val cache = mutable.Map[String, ExecutorAndDepth]()
    executorMap.foreach { case (selectWord, executor) => addInGraph(graph, selectWord, cache, Set(executor)) }
    graph.get
  }

  /**
    * Recursively traverse the graph from the input node up to the root
    * and propagate the executors associated to that node toward the top,
    * except if nodes present in the executor map are encountered.
    * In the latter case, use the executor of one of the closest of such encountered nodes.
    */
  private def addInGraph(graph: mutable.Map[String, Set[ExecutorInvoker]],
                         selectWord: String,
                         cache: mutable.Map[String, ExecutorAndDepth],
                         executors: Set[ExecutorInvoker],
                         currentCycle: Set[String] = Set()): Unit = {
    assert(!currentCycle.contains(selectWord),
      "The POSet doesn't have a strict order, there is a cycle in:\n" + currentCycle.mkString(", "))

    // go to the top of the graph to find an executor, and cache everything on the way
    val updatedExecutors = getOriginalExecutor(selectWord, cache).map(Set(_)).getOrElse {
      // there was no original executor neither above nor here, so let's add
      // the executors of the calling child here (we will take all children to cover it).
      graph.get(selectWord).map(_ ++ executors).getOrElse(executors)
    }

    graph(selectWord) = updatedExecutors

    lazy val updatedCycle = currentCycle + selectWord
    getParents(selectWord).filterNot(executorMap.contains).foreach(
      addInGraph(graph, _, cache, updatedExecutors, updatedCycle)
    )
  }
}


object ExecutorsByPoset { // for an easier access from Groovy scripts
  def apply(executorMap: java.util.Map[String, ExecutorInvoker], getParents: Closure[java.lang.Iterable[String]]): ExecutorsByPoset = {
    new ExecutorsByPoset(executorMap.asScala.toMap, getParents.call(_).asScala)
  }
}
