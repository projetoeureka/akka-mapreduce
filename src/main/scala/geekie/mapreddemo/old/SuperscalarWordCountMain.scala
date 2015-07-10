package geekie.mapreddemo.old

import akka.actor._
import geekie.mapred.PipelineHelpers._
import geekie.mapred._
import geekie.mapred.io.{FileChunk, FileChunks}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Created by nlw on 05/04/15.
 * Akka based map-reduce task example with neat job flow control.
 *
 */
object SuperscalarWordCountMain extends App {
  println("RUNNING \"SUPERSCALAR\" M/R")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    val system = ActorSystem("akka-wordcount")

    val wordcountSupervisor = system.actorOf(Props[SsWcMapReduceSupervisor], "wc-super")
    wordcountSupervisor ! MultipleFileReaders(args(0))
    // wcSupAct ! SingleFileReader(args(0))
  }
}

class SsWcMapReduceSupervisor extends Actor {

  type A = String
  type RedK = String
  type RedV = Int

  val nMappers = 4
  val nReducers = 8
  val nChunks = nMappers * 4

  val myworkers = PipelineStart[String] map {
    ss => ss split raw"\s+"
  } times nMappers map {
    word: String => Some(word.trim.toLowerCase.filterNot(_ == ','))
  } times 4 map {
    word: String => if (StopWords contains word) None else Some(word)
  } times 4 map {
    word: String => Some(KeyVal(word, 1))
  } times 4 reduce (_ + _) times nReducers output self

  val mapper = myworkers.head

  var progress = 0
  var finalAggregate: Map[RedK, RedV] = Map()

  def receive = {
    case MultipleFileReaders(filename) =>
      println(s"PROCESSING FILE $filename")
      FileChunks(filename, nChunks).zipWithIndex foreach {
        case (chunk, n) => mapper ! FileChunk(chunk, n)
      }

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[RedK, RedV]]
      finalAggregate = finalAggregate ++ ag

    case ProgressReport(n) =>
      progress += 1
      println(f"CHUNK $n%2d - $progress%2d of $nChunks")
      if (progress == nChunks) mapper ! ForwardToReducer(EndOfData)

    case EndOfData =>
      PrintWordcountResults(finalAggregate)
      context.system.scheduler.scheduleOnce(1.second, self, SsWcMapReduceSupervisor.HammerdownProtocol)

    case SsWcMapReduceSupervisor.HammerdownProtocol => context.system.shutdown()
  }
}

object SsWcMapReduceSupervisor{
  case object HammerdownProtocol
}

object SsWcPrintResults {
  def apply[RedK, RedV](finalAggregate: Map[RedK, RedV]) = {
    println("FINAL RESULTS")
    val ag = finalAggregate.asInstanceOf[Map[String, Int]]
    ag.toList sortBy (-_._2) take 20 foreach {
      case (s, i) => println(f"$s%8s:$i%5d")
    }
  }
}
