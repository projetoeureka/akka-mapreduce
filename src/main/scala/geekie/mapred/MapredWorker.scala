package geekie.mapred

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.{ActorRefRoutee, RemoveRoutee}
import geekie.mapred.MapredWorker._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

class MapredWorker[A: ClassTag, K: ClassTag, V: ClassTag](mf: A => TraversableOnce[KeyVal[K, V]],
                                                          rf: (V, V) => V,
                                                          ff: Map[K, V] => Unit) extends Actor {

  var acc = Map.empty[K, V]

  def aggregateNewData(kvs: TraversableOnce[KeyVal[K, V]]) = kvs foreach {
    case KeyVal(k, v) =>
      acc += (k -> (if (acc contains k) rf(acc(k), v) else v))
  }

  def receive = {
    case DataChunk(xs, req) =>
      val kvs = xs.asInstanceOf[TraversableOnce[A]] flatMap mf
      aggregateNewData(kvs)
      req ! ChunkAck

    case KeyValChunk(kvs, req) =>
      aggregateNewData(kvs.asInstanceOf[TraversableOnce[KeyVal[K, V]]])
      req ! KeyValChunkAck

    case MultiplyAndSurrender(n, requester) if n > 0 =>
      acc.toIterator
        .map({ case (k, v) => KeyVal(k, v) })
        .grouped(math.max(1, (acc.size + n - 1) / n)).toIterator
        .foreach(kvs => context.parent ! KeyValChunk(kvs, requester))
      context.parent ! RemoveRoutee(ActorRefRoutee(self))
      context become surrender

    case MultiplyAndSurrender(n, requester) if n <= 0 =>
      requester ! ResultData(acc)
      context.parent ! RemoveRoutee(ActorRefRoutee(self))
      context become surrender
  }

  def surrender: Receive = {
    case x =>
      import context.dispatcher
      context.system.scheduler.scheduleOnce(100 millis, context.parent, x)
  }
}

object MapredWorker {

  case class KeyVal[K, V](key: K, value: V)

  case class DataChunk[A](chunk: TraversableOnce[A], requester: ActorRef)

  case class KeyValChunk[K, V](kvChunk: TraversableOnce[KeyVal[K, V]], requester: ActorRef)

  case object ChunkAck

  case object KeyValChunkAck

  case class MultiplyAndSurrender(step: Int, requester: ActorRef)

  case class ResultData[K, V](data: Map[K, V])

  def props[A: ClassTag, K: ClassTag, V: ClassTag](mf: A => TraversableOnce[KeyVal[K, V]],
                                                   rf: (V, V) => V,
                                                   ff: Map[K, V] => Unit) =
    Props(new MapredWorker(mf, rf, ff))
}
