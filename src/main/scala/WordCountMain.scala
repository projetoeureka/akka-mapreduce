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

    val wordcountSupervisor = system.actorOf(Props[WordcountSupervisor], "wc-super")

    wordcountSupervisor ! BeginJob(args(0))
  }
}


class WordcountSupervisor extends Actor {

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

  val dataSource = context.actorOf(Props(classOf[FileReader], mapper, nMappers * 2), "wc-super")

  var finalAggregate: Map[RedK, RedV] = Map()

  def receive = {
    case BeginJob(filename) =>
      dataSource ! SplitFile(filename, 20)
    case msg: ProgressReport =>
      dataSource forward msg
    case ReducerResult(agAny) =>
      finalAggregate ++= agAny.asInstanceOf[Map[RedK, RedV]]
    case EndOfData =>
      PrintResults(finalAggregate)
      context.system.scheduler.scheduleOnce(2.second, self, HammerdownProtocol)
    case HammerdownProtocol =>
      context.system.shutdown()
  }
}

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

case class BeginJob(filename: String)

case object HammerdownProtocol
