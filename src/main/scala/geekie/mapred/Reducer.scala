package geekie.mapred

import akka.actor.Actor
import akka.routing.ConsistentHashingRouter.ConsistentHashable

/**
 * Created by nlw on 08/04/15.
 * A reducer that aggregates objects associated to a certain key in a Map.
 */
case object ReducerFinish

case class ReducerResult[K, V](aggregator: Map[K, V])

class Reducer[K, V](f: (V, V) => V) extends Actor {
  var aggregator: Map[K, V] = Map()

  def receive = {
    case KeyVal(key: K, value: V) => {
      aggregator += (key -> (if (aggregator contains key) f(aggregator(key), value) else value))
    }
    case ReducerFinish => {
      sender ! ReducerResult(aggregator)
      context.stop(self)
    }
  }
}


case class KeyVal[K, V](key: K, value: V) extends ConsistentHashable {
  override def consistentHashKey: Any = key
}
