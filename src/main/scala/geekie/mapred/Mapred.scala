package geekie.mapred

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{MaxInFlightRequestStrategy, ActorSubscriber}
import geekie.mapred.MapredWorker._

import scala.reflect.ClassTag

class Mapred[A, K, V](nWorkers: Int,
                      mf: A => TraversableOnce[KeyVal[K, V]],
                      rf: (V, V) => V) extends ActorSubscriber with ActorLogging {

  println("reouterrrr")

  def mapredWorkerProps = Props(classOf[MapredWorker[A, K, V]], mf, rf)

  val MAX_QUEUE_SIZE = 10
  var queue = Set.empty[Int]
  var nChunksDone = 0

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MAX_QUEUE_SIZE) {
    override def inFlightInternally: Int = queue.size
  }

  // val mapredRouter = context.actorOf(BalancingPool(nWorkers).props(mapredWorkerProps), "mapred-router")
  val mapredRouter = context.actorOf(RoundRobinPool(nWorkers).props(mapredWorkerProps), "mapred-router")

  def receive = {
    case OnNext(cc: DataChunk[A]) =>
      log.info(s"rcvd ${cc.id}")
      queue += cc.id
      mapredRouter ! cc

    case ChunkAck(n) =>
      log.info(s"DONE: $n ($nChunksDone total) ${}")
      queue -= 1
      nChunksDone += 1

    case OnError(err: Exception) =>
      log.error(err.toString)

    case OnComplete =>
      log.info("AKKABOU")
      log.info("REDUZIR OS REDUCERS AGORA...")
      mapredRouter ! MultiplyAndSurrender(4, 100, self)
      mapredRouter ! MultiplyAndSurrender(4, 101, self)
      mapredRouter ! MultiplyAndSurrender(4, 102, self)
      mapredRouter ! RequestResult(self)

    case ResultData(acc) =>
      println(acc)

  }

}

object Mapred {
  def apply[A: ClassTag, K: ClassTag, V: ClassTag](nWorkers: Int)
                                                  (mf: A => TraversableOnce[KeyVal[K, V]])
                                                  (rf: (V, V) => V)
                                                  (implicit context: akka.actor.ActorContext) =
    context.actorOf(Props(new Mapred[A, K, V](nWorkers, mf, rf)), s"mapred-supervisor")
}
