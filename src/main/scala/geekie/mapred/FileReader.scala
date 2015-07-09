package geekie.mapred

import akka.actor.{ActorLogging, Actor, ActorRef}
import geekie.mapred.io.{DataChunk, FileChunk, LimitedEnumeratedFileChunks}

class FileReader(output: ActorRef,
                 filename: String,
                 nChunks: Int,
                 chunkWindow: Int,
                 sampleSize: Option[Int]) extends Actor with ActorLogging {

  log.info(s"SAMPLING FILE $filename")
  val allChunks = LimitedEnumeratedFileChunks(filename, nChunks, sampleSize)
  for (cc <- allChunks.take(chunkWindow)) output ! cc

  def receive = chunkEmitter(allChunks.drop(chunkWindow), List())

  def chunkEmitter[T](chunks: Stream[FileChunk], done: List[Int]): Receive = {
    case ProgressReport(n) =>
      chunks.take(1) foreach (output ! _)
      val nDone = 1 + done.length
      logProgress(n, nDone)
      if (nDone == nChunks) output ! ForwardToReducer(EndOfData)
      context.become(chunkEmitter(chunks.drop(1), n :: done))
  }

  def logProgress(n: Int, nDone: Int) =
    log.info(f"CHUNK $n%2d - $nDone%2d of $nChunks (${nDone * 100.0 / nChunks}%.1f%%)")
}


