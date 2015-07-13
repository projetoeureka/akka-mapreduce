package geekie.mapred

import akka.actor.{Actor, ActorRef}
import geekie.mapred.MapredWorker._

import scala.reflect.ClassTag

class MapredWorker[A: ClassTag, K: ClassTag, V: ClassTag](mf: A => TraversableOnce[KeyVal[K, V]], rf: (V, V) => V) extends Actor {


  println("oiiii")


  var acc = Map.empty[K, V]
  val ACC_SPLIT_FACTOR = 10

  def receive = {
    case DataChunk(xs, id, req) =>
      self ! KeyValChunk(xs.asInstanceOf[TraversableOnce[A]].toIterator flatMap mf, id, req)

    case KeyValChunk(kvs, id, req) =>
      for (KeyVal(k, v) <- kvs.asInstanceOf[TraversableOnce[KeyVal[K, V]]])
        acc = acc + (k -> (if (acc contains k) rf(acc(k), v) else v))
      req ! ChunkAck(id)

    case MultiplyAndSurrender(pieces, id, requester) =>
      acc.toIterator
        .map({ case (k, v) => KeyVal(k, v) })
        .grouped(math.max(1, acc.size / pieces)).toIterator
        .foreach(kvs => context.parent ! KeyValChunk(kvs, id, requester))

    case RequestResult(requester) =>
      requester ! ResultData(acc)
      context stop self
  }
}

object MapredWorker {
//  def apply[A: ClassTag, K: ClassTag, V: ClassTag](mf: A => TraversableOnce[KeyVal[K, V]])(rf: (V, V) => V) =
//    new MapredWorker[A, K, V](mf, rf)

  case class KeyVal[K, V](key: K, value: V)

  case class DataChunk[A](chunk: TraversableOnce[A], id: Int, requester: ActorRef)

  case class KeyValChunk[K, V](kvChunk: TraversableOnce[KeyVal[K, V]], id: Int, requester: ActorRef)

  case class ChunkAck[A](id: Int)

  case class MultiplyAndSurrender(pieces: Int, id: Int, requester: ActorRef)

  case class RequestResult(requester: ActorRef)

  case class ResultData[K, V](data: Map[K, V])
}

