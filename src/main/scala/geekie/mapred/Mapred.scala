package geekie.mapred

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{MaxInFlightRequestStrategy, ActorSubscriber}
import geekie.mapred.MapredWorker._
import geekie.mapreddemo.PrintWordcountResults

import scala.reflect.ClassTag

class Mapred[A: ClassTag, K: ClassTag, V: ClassTag](nWorkers: Int,
                                                    mf: A => TraversableOnce[KeyVal[K, V]],
                                                    rf: (V, V) => V) extends ActorSubscriber with ActorLogging {

  val MAX_QUEUE_SIZE = 10
  var queue = 0
  var nChunksDone = 0

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MAX_QUEUE_SIZE) {
    override def inFlightInternally: Int = queue
  }

  def workerProps = SmallestMailboxPool(nWorkers).props(MapredWorker.props(mf, rf))

  val mapredRouter = context.actorOf(workerProps, "mapred-router")

  def receive = {
    case OnNext(cc: TraversableOnce[A]) =>
      queue += 1
      mapredRouter ! DataChunk(cc, self)

    case ChunkAck =>
      //      log.info(s"DONE: $n ($nChunksDone total) ${}")
      queue -= 1
      nChunksDone += 1

    case OnError(err: Exception) =>
      log.error(err.toString)

    case OnComplete =>
      log.info("AKKABOU")
      log.info("REDUZIR OS REDUCERS AGORA...")
      mapredRouter ! MultiplyAndSurrender(nWorkers, self)

    case ResultData(acc) =>
      log.info("RESULT:\n" + acc)
      PrintWordcountResults(acc.asInstanceOf[Map[K, Int]])
  }
}

object Mapred {
  def props[A: ClassTag, K: ClassTag, V: ClassTag](nWorkers: Int)
                                                  (mf: A => TraversableOnce[KeyVal[K, V]])
                                                  (rf: (V, V) => V)
                                                  (implicit context: akka.actor.ActorContext) =
    Props(new Mapred[A, K, V](nWorkers, mf, rf))
}

