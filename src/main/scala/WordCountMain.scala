import akka.actor._
import geekie.mapred._
import geekie.mapred.io.FileChunks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

import PipelineHelpers._

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
    wordcountSupervisor ! MultipleFileReaders(args(0))
  }
}

class MapReduceSupervisor extends Actor {

  type A = String
  type RedK = String
  type RedV = Int

  val nMappers = 4
  val nReducers = 8
  val nChunks = nMappers * 4

  val myWorkers = Pipeline[String, String](List()) map { ss =>
    (ss split raw"\s+")
      .map(word => word.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map(KeyVal(_, 1))
  } times nMappers reduce (_ + _) times nReducers output self

  val mapper = myWorkers.head

  var progress = 0
  var finalAggregate: Map[RedK, RedV] = Map()

  def receive = {
    case MultipleFileReaders(filename) =>
      println(s"PROCESSING FILE $filename")
      FileChunks(filename, nChunks).zipWithIndex foreach {
        case (chunk, n) => mapper ! DataChunk(chunk.iterator, n)
      }

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[RedK, RedV]]
      finalAggregate = finalAggregate ++ ag

    case ProgressReport(n) =>
      progress += 1
      println(f"CHUNK $n%2d - $progress%2d of $nChunks")
      if (progress == nChunks) mapper ! Forward(EndOfData)

    case EndOfData =>
      PrintResults(finalAggregate)
      context.system.scheduler.scheduleOnce(1.second, self, HammerdownProtocol)

    case HammerdownProtocol => context.system.shutdown()
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
