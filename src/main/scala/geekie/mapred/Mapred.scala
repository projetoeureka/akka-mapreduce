package geekie.mapred

import akka.actor.{ActorLogging, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy}
import geekie.mapred.MapredWorker._

import scala.reflect.ClassTag

/** Mapred ActorSubscriber receives a function to flatMap the input producing KeyVal objects, then a function to
  * reduceByKey these values, and then a finalization function that receives this result.
  *
  * @param nWorkers Number of worker actors to instantiate inside the router.
  * @param mf Mapping function. Actually "flatMap". Should return a `TraversableOnce[KeyVal[_,_]]`.
  * @param rf Reducing function.
  * @param ff Finalize function.
  * @tparam A input type, should probably be ByteString.
  * @tparam K Key to reduce over.
  * @tparam V values that will be reduced. Usually a monoid.
  */
class Mapred[A: ClassTag, K: ClassTag, V: ClassTag](nWorkers: Int,
                                                    mf: A => TraversableOnce[KeyVal[K, V]],
                                                    rf: (V, V) => V,
                                                    ff: Map[K, V] => Unit) extends ActorSubscriber with ActorLogging {

  val MAX_QUEUE_SIZE = 2 * nWorkers
  var queue = 0
  var nChunksDone = 0

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MAX_QUEUE_SIZE) {
    override def inFlightInternally: Int = queue
  }

  def workerProps = SmallestMailboxPool(nWorkers).props(MapredWorker.props(mf, rf, ff))

  val mapredRouter = context.actorOf(workerProps, "mapred-router")

  def receive = {
    case OnNext(cc: TraversableOnce[A]) =>
      queue += 1
      mapredRouter ! DataChunk(cc, self)

    case OnError(err: Exception) =>
      log.error(err.toString)

    case OnComplete =>
      log.info("FINISHING")
      mapredRouter ! MultiplyAndSurrender(nWorkers - 1, self)
      context.become(reduceReducers(nWorkers - 1))

    case ChunkAck =>
      queue -= 1
      nChunksDone += 1
      log.info(s"DONE: $nChunksDone chunks")
  }

  def reduceReducers(toAck: Int, acked: Int = 0): Receive = {
    case ChunkAck =>
      queue -= 1
      nChunksDone += 1
      log.info(s"DONE: $nChunksDone chunks")

    case KeyValChunkAck =>
      if (acked + 1 < toAck)
        context.become(reduceReducers(toAck, acked + 1))
      else {
        mapredRouter ! MultiplyAndSurrender(toAck - 1, self)
        context.become(reduceReducers(toAck - 1))
      }

    case ResultData(acc) =>
      ff(acc.asInstanceOf[Map[K, V]])
  }
}

object Mapred {
  def props[A: ClassTag, K: ClassTag, V: ClassTag](nWorkers: Int)
                                                  (mf: A => TraversableOnce[KeyVal[K, V]])
                                                  (rf: (V, V) => V)
                                                  (ff: Map[K, V] => Unit)
                                                  (implicit context: akka.actor.ActorContext) =
    Props(new Mapred(nWorkers, mf, rf, ff))
}
