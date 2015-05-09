package geekie.mapred

import akka.actor.{Actor, ActorRef}
import geekie.mapred.io.{DataChunk, LimitedEnumeratedFileChunks}

class FileReader(output: ActorRef, chunkWindow: Int) extends Actor {

  var nChunks: Int = _
  var chunkIterator: Iterator[DataChunk[String]] = _
  var sentChunks = 0
  var finishedChunks = 0

  def sendChunk() = sendChunks(1)

  def sendChunks(n: Int = 1) = chunkIterator.take(n) foreach { chunk =>
    output ! chunk
    sentChunks += 1
  }

  def printProgress(n: Int) = println(f"CHUNK $n%2d - $finishedChunks%2d of $nChunks (${finishedChunks * 100.0 / nChunks}%.1f%%)")

  def receive = {
    case SplitFile(filename, nChunks_, sampleSize) =>
      println(s"SAMPLING FILE $filename")
      nChunks = nChunks_
      chunkIterator = LimitedEnumeratedFileChunks(filename, nChunks, sampleSize).iterator
      sendChunks(chunkWindow)

    case ProgressReport(n) =>
      if (sentChunks < nChunks) sendChunk()
      finishedChunks += 1
      printProgress(n)
      if (finishedChunks == nChunks) output ! ForwardToReducer(EndOfData)
  }
}
