package geekie.mapred.utils

case class Counter[K, V](counter: Map[K, V], f: (V, V) => V) {
/*
  def +(otherCounter: Counter[K, V]) = {
    val mine = for ((k, v) <- counter) yield {
      val wOpt = otherCounter.counter.get(k)
      k -> (if (wOpt.isDefined) f(v, wOpt.get) else v)
    }

    val hers = otherCounter.counter filterKeys (k => !(counter contains k))
    Counter(mine ++ hers, f)
  }
*/

  def addFromMap (otherCounter: Map[K, V]) = {
    val mine = for ((k, v) <- counter) yield {
      val wOpt = otherCounter.get(k)
      k -> (if (wOpt.isDefined) f(v, wOpt.get) else v)
    }

    val hers = otherCounter filterKeys (k => !(counter contains k))
    Counter(mine ++ hers, f)
  }
}

object Counter {
  def apply[K, V](f: (V, V) => V): Counter[K, V] = Counter(Map[K, V](), f)
}
