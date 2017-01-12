package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker


class TargetDispatchingByPoset(val executorsByPoset: ExecutorsByPoset) extends TargetDispatching {
  // todo? support multiple executors per target, possibly with the choice between AnyOf[ExecutorType] and AllOf[ExecutorType]

  override def dispatch(select: Select): Iterator[(ExecutorInvoker, Select)] = {
    (for {
      word <- select
      executor <- executorsByPoset.getExecutors(Some(word))
    } yield (word, executor))
      .groupBy(_._2)
      .toIterator // very important to not let the keys in the map below override each other
      .map({ case (executor, words) => (executor, words.map(_._1)) })
  }
}
