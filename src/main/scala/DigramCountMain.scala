import akka.actor._
import geekie.mapred._
import geekie.mapred.io.FileChunks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Created by nlw on 05/04/15.
 * Akka based map-reduce task example with neat job flow control.
 *
 */
object DigramCountMain extends App {
  println("RUNNING DIGRAM M/R")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    val system = ActorSystem("akka-wordcount")

    val wordcountSupervisor = system.actorOf(Props[DigramCountSupervisor], "wc-super")
    wordcountSupervisor ! MultipleFileReaders(args(0))
  }
}

class DigramCountSupervisor extends Actor {

  type A = String
  type RedK = String
  type RedV = Int

  val nMappers = 4
  val nReducers = 8
  val nChunks = nMappers * 8

  val myWorkers = pipe_mapkv { rr: String =>
    val ss = rr.trim.toLowerCase
    for (dig <- ss zip (ss.drop(1) + "\n")) yield KeyVal("" + dig._1 + dig._2, 1)
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
