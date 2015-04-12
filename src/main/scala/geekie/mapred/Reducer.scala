/**
 * Created by nlw on 08/04/15.
 * A reducer that aggregates objects associated to a certain key in a Map.
 *
 */
package geekie.mapred

import akka.actor.{ActorRef, Actor}
import akka.routing.ConsistentHashingRouter.ConsistentHashable

import scala.reflect.ClassTag

case object GetAggregator

case class ReducerResult[K, V](aggregator: Map[K, V])

case class KeyVal[K, V](key: K, value: V) extends ConsistentHashable {
  override def consistentHashKey: Any = key
}

class Reducer[K: ClassTag, V: ClassTag](output: ActorRef, f: (V, V) => V) extends Actor {
  var aggregator: Map[K, V] = Map()

  def receive = {
    case KeyVal(key: K, value: V) =>
      aggregator += (key -> (if (aggregator contains key) f(aggregator(key), value) else value))

    case GetAggregator =>
      output ! ReducerResult(aggregator)

    case Forward(x) =>
      output ! x
  }
}

object Reducer {
  def apply[K: ClassTag, V: ClassTag](output: ActorRef)(f: (V, V) => V) = new Reducer[K, V](output, f)
}
