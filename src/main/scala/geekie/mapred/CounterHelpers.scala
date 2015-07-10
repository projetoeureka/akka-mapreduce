package geekie.mapred

object CounterHelpers {
  implicit class MergeMethod[K](counter: Map[K, Int]) {
    def +(otherCounter: Map[K, Int]) = {
      val mine = for ((k, v) <- counter) yield k -> (v + otherCounter.getOrElse(k, 0))
      val hers = otherCounter filterKeys (k => !(counter contains k))
      mine ++ hers
    }
  }
}
