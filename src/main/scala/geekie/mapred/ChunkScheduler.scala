package geekie.mapred

import akka.actor.{Actor, ActorLogging, ActorRef}
import geekie.mapred.io.DataChunk

class ChunkScheduler[A](output: ActorRef,
                        allChunks: TraversableOnce[DataChunk[A]],
                        chunkWindow: Int,
                        nChunks: Option[Int]
                         ) extends Actor with ActorLogging {

  val chunksItr = allChunks.toIterator

  var sent: Int = 0

  for (cc <- chunksItr.take(chunkWindow)) {
    output ! cc
    sent += 1
  }

  def receive = chunkEmitter(chunksItr, Set())

  def chunkEmitter(chunks: Iterator[DataChunk[A]], done: Set[Int]): Receive = {
    case ProgressReport(n) =>
      val newDone = done + n
      logProgress(n, newDone.size)
      chunks.take(1) foreach { cc =>
        output ! cc
        sent += 1
      }
      if ((!chunksItr.hasNext) && sent == newDone.size) output ! ForwardToReducer(EndOfData)
      context.become(chunkEmitter(chunks, newDone))
  }

  def logProgress(n: Int, nDone: Int) = log.info(
    f"CHUNK $n%2d - $nDone%2d of " + (nChunks match {
      case Some(tot) => f"$tot (${nDone * 100.0 / tot}%.1f%%)"
      case None => "?"
    })
  )
}
