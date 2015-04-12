package geekie.mapred

import akka.routing.ConsistentHashingRouter.ConsistentHashable

import scala.reflect.ClassTag

/**
 * Created by nlw on 12/04/15.
 * Messages used in the Map/Reduce framework.
 *
 */
case class Forward[T](obj: T)

case class KeyVal[K, V](key: K, value: V) extends ConsistentHashable {
  override def consistentHashKey = key
}

case object GetAggregator

case class ReducerResult[K: ClassTag, V: ClassTag](aggregator: Map[K, V])

