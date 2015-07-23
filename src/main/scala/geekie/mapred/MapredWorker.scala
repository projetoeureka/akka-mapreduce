package geekie.mapred

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.{ActorRefRoutee, RemoveRoutee}
import geekie.mapred.MapredWorker._

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag


class MapredWorker[A: ClassTag, K: ClassTag, V: ClassTag](mf: A => TraversableOnce[MapredWorker.KeyVal[K, V]],
                                                          rf: (V, V) => V,
                                                          ff: Map[K, V] => Unit) extends Actor {

  type Chunk = TraversableOnce[A]
  type KeyValChunk = TraversableOnce[KeyVal[K, V]]

  implicit class TraversableChunkenizer[X](input: Traversable[X]) {
    /** Split a container into a given number of chunks, with empty chunks if necessary */
    def chunkenize(chunks: Int): Stream[Traversable[X]] =
      if (chunks > 1) {
        val (aa, bb) = input splitAt (input.size / chunks)
        aa #:: bb.chunkenize(chunks - 1)
      } else Stream(input)
  }

  implicit class CounterAggregator(input: Map[K, V]) {
    /** Aggregates a list of key-value pairs into the total reducer accumulator */
    def agg(kvs: KeyValChunk): Map[K, V] =
      (input /: kvs) { case (acc, KeyVal(k, v)) =>
        acc + (k -> (if (acc contains k) rf(acc(k), v) else v))
      }
  }

  override def receive = receivingData(Map.empty[K, V])

  def receivingData(acc: Map[K, V]): Receive = {
    case DataChunk(chunk: Chunk, req) =>
      val newAcc = acc agg (chunk flatMap mf)
      req ! ChunkAck
      context become receivingData(newAcc)

    case KeyValChunk(kvChunkErased, req) =>
      val newAcc = acc agg kvChunkErased.asInstanceOf[KeyValChunk]
      context become receivingData(newAcc)
      req ! KeyValChunkAck

    case FinishWorkers(n, requester) if n > 0 =>
      acc.map({ case (k, v) => KeyVal(k, v) })
        .chunkenize(n)
        .foreach(kvs => context.parent ! KeyValChunk(kvs, requester))
      context.parent ! RemoveRoutee(ActorRefRoutee(self))
      context become finishing

    case FinishWorkers(0, requester) =>
      requester ! ResultData(acc)
      context.parent ! RemoveRoutee(ActorRefRoutee(self))
      context become finishing
  }

  def finishing: Receive = {
    case x =>
      import context.dispatcher
      val BACKOFF_DELAY = 100.millis
      context.system.scheduler.scheduleOnce(BACKOFF_DELAY, context.parent, x)
  }
}


object MapredWorker {

  case class KeyVal[K, V](key: K, value: V)

  case class DataChunk[A](chunk: TraversableOnce[A], requester: ActorRef)

  case class KeyValChunk[K, V](kvChunk: TraversableOnce[KeyVal[K, V]], requester: ActorRef)

  case object ChunkAck

  case object KeyValChunkAck

  case class FinishWorkers(workersLeft: Int, requester: ActorRef)

  case class ResultData[K, V](data: Map[K, V])

  def props[A: ClassTag, K: ClassTag, V: ClassTag](mf: A => TraversableOnce[KeyVal[K, V]],
                                                   rf: (V, V) => V,
                                                   ff: Map[K, V] => Unit) =
    Props(new MapredWorker(mf, rf, ff))
}
