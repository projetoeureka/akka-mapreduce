import akka.actor._
import geekie.mapred.PipelineHelpers._
import geekie.mapred._
import geekie.mapred.io.{DataChunk, LimitedEnumeratedFileChunks}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

/**
 * Created by nlw on 05/04/15.
 * Akka based map-reduce task example with neat job flow control.
 *
 */
object WordCountMain extends App {
  println("RUNNING NEAT M/R")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    val system = ActorSystem("akka-wordcount")

    val wordcountSupervisor = system.actorOf(Props[MapReduceSupervisor], "wc-super")
    wordcountSupervisor ! FileSplitter(args(0), 20)
    // wordcountSupervisor ! FileSplitter(args(0), 100, Some(2))
  }
}

class MapReduceSupervisor extends Actor {

  type A = String
  type RedK = String
  type RedV = Int

  val nMappers = 4
  val nReducers = 8

  val myWorkers = PipelineStart[String] map { ss =>
    (ss split raw"\s+")
      .map(word => word.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map(KeyVal(_, 1))
  } times nMappers reduce (_ + _) times nReducers output self
  val mapper = myWorkers.head

  var finalAggregate: Map[RedK, RedV] = Map()

  val chunkWindow = nMappers * 2
  var nChunks: Int = _
  var chunkIterator: Iterator[DataChunk[String]] = _
  var sentChunks = 0
  var progress = 0

  def sendChunks(n: Int = 1) = chunkIterator.take(n) foreach { chunk =>
    mapper ! chunk
    sentChunks += 1
  }

  def sendChunk() = sendChunks(1)

  def printProgress(n: Int) = println(f"CHUNK $n%2d - $progress%2d of $nChunks (${progress * 100.0 / nChunks}%.1f%%)")

  def receive = {
    case FileSplitter(filename, nChunks_, sampleSize) =>
      println(s"SAMPLING FILE $filename")
      nChunks = nChunks_
      chunkIterator = LimitedEnumeratedFileChunks(filename, nChunks, sampleSize).iterator
      sendChunks(chunkWindow)

    case ProgressReport(n) =>
      if (sentChunks < nChunks) sendChunk()
      progress += 1
      printProgress(n)
      if (progress == nChunks) mapper ! ForwardToReducer(EndOfData)

    case ReducerResult(agAny) =>
      finalAggregate ++= agAny.asInstanceOf[Map[RedK, RedV]]

    case EndOfData =>
      PrintResults(finalAggregate)
      context.system.scheduler.scheduleOnce(1.second, self, HammerdownProtocol)
  }
}

case class MultipleFileReaders(filename: String)

case object HammerdownProtocol

object StopWords {
  private val stopWords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopWords.contains(s)
}

object PrintResults {
  def apply[RedK, RedV](finalAggregate: Map[RedK, RedV]) = {
    println("FINAL RESULTS")
    val ag = finalAggregate.asInstanceOf[Map[String, Int]]
    ag.toList sortBy (-_._2) take 20 foreach {
      case (s, i) => println(f"$s%8s:$i%5d")
    }
  }
}
