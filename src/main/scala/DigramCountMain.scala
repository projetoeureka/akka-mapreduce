package MapRedDemo

import akka.actor._
import geekie.mapred.PipelineHelpers._
import geekie.mapred._
import geekie.mapred.io.{DataChunk, FileChunkLineReader, FileChunks}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Created by nlw on 05/04/15.
 * Akka based map-reduce task example with neat job flow control.
 *
 */
object DigramCountMain extends App {
  println("RUNNING DIGRAM M/R")
  if (args.length < 2) println("MISSING INPUT FILENAME OR CHUNKS WINDOW SIZE")
  else {
    val system = ActorSystem("akka-wordcount")

    val wordcountSupervisor = system.actorOf(Props[DigramCountSupervisor], "wc-super")
    wordcountSupervisor ! MultipleFileReadersWindow(args(0), args(1).toInt)
  }
}

class DigramCountSupervisor extends Actor {

  type A = String
  type RedK = String
  type RedV = Int

  val nMappers = 8
  val nReducers = 8
  val nChunks = nMappers * 8

  val myWorkers = PipelineStart[String] map { rr =>
    val ss = rr.trim.toLowerCase
    for (dig <- ss zip (ss.drop(1) + "\n"); n <- 1 to 10)
      yield KeyVal("" + dig._1 + dig._2, 1)
  } times nMappers reduce (_ + _) times nReducers output self

  val mapper = myWorkers.head

  var progress = 0
  var finalAggregate: Map[RedK, RedV] = Map()

  var chunkIterator: Iterator[(FileChunkLineReader, Int)] = _

  def sendChunk(count: Int = 1) = {
    val results = for {
      (chunk, n) <- chunkIterator.take(count)
    } yield mapper ! DataChunk(chunk, n)
    results.length
  }

  def receive = {
    case MultipleFileReadersWindow(filename, simchunks) =>
      println(s"PROCESSING FILE $filename")
      chunkIterator = FileChunks(filename, nChunks).zipWithIndex.iterator
      sendChunk(simchunks)

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[RedK, RedV]]
      finalAggregate = finalAggregate ++ ag

    case ProgressReport(n) =>
      sendChunk(1)
      progress += 1
      println(f"CHUNK $n%2d - $progress%2d of $nChunks")
      if (progress == nChunks) mapper ! Forward(EndOfData)

    case EndOfData =>
      PrintWordcountResults(finalAggregate)
      context.system.scheduler.scheduleOnce(1.second, self, DigramCountSupervisor.HammerdownProtocol)

    case DigramCountSupervisor.HammerdownProtocol => context.system.shutdown()
  }
}

object DigramCountSupervisor {
  case object HammerdownProtocol
}

case class MultipleFileReadersWindow(filename: String, simchunks: Int)
