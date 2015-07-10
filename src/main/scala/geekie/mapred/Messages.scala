package geekie.mapred

import akka.routing.ConsistentHashingRouter.ConsistentHashable

import scala.reflect.ClassTag

/**
 * Created by nlw on 12/04/15.
 * Messages used in the Map/Reduce framework.
 *
 */
case class Forward[T](obj: T)

case class ForwardToReducer[T](obj: T)

trait Reducible[K, V]

case class KeyVal[K, V](key: K, value: V) extends Reducible[K, V]

case class KeyValTraversable[K, V](kvs: TraversableOnce[KeyVal[K, V]]) extends Reducible[K, V]

case class MultipleFileReaders(filename: String)

case class SplitFile(filename: String, nChunks: Int, chunkMaxSize: Option[Int] = None)

case class ProgressReport(n: Int)

case object GetAggregator

case class ReducerResult[K: ClassTag, V: ClassTag](aggregator: Map[K, V])

case object EndOfData
