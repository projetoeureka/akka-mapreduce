import akka.actor._
import akka.routing._
import geekie.mapred._
import geekie.mapred.io.{FileChunks, FileSize}

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
    val wordcountSupervisor = system.actorOf(Props(classOf[WordCountSupervisor], 8, 8), "wc-super")
    wordcountSupervisor ! MultipleFileReaders(args(0))
    // wcSupAct ! SingleFileReader(args(0))
  }
}

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {

  val reducer = context.actorOf(Props(classOf[Reducer], self, nReducers), "reducer")
  val mapper = context.actorOf(Props(classOf[Mapper], reducer, nMappers), "mapper")

  var progress = 0
  var finalAggregate: Map[String, Int] = Map()

  def receive = {
    case MultipleFileReaders(filename) =>
      println(s"PROCESSING FILE $filename")
      FileChunks(filename, nMappers) foreach (mapper ! _.iterator)
      mapper ! EndOfData

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag

    case EndOfData =>
      PrintResults(finalAggregate)
      context.system.scheduler.scheduleOnce(1.second, self, HammerdownProtocol)

    case HammerdownProtocol => context.system.shutdown()
  }
}

class Mapper(output: ActorRef, nMappers: Int) extends Actor {
  val mapperDecimator = context.actorOf(Props(classOf[Decimator], output, nMappers, EndOfData), "mapper-decimator")

  def wordEmitter = { ss: String =>
    ss.split(raw"\s+").toSeq
      .map(_.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map { ww => KeyVal(ww, 1) }
  }

  def myMapper = MapperTask(mapperDecimator)(wordEmitter)

  val mapperRouter = context.actorOf(SmallestMailboxPool(nMappers).props(Props(myMapper)), "mapper-router")

  def receive = {
    case EndOfData => mapperRouter ! Broadcast(Forward(EndOfData))
    case x: Any => mapperRouter ! x
  }
}

class Reducer(output: ActorRef, nReducers: Int) extends Actor {
  val reducerDecimator = context.actorOf(Props(classOf[Decimator], output, nReducers, EndOfData), "reducer-decimator")

  def myReducer = ReducerTask[String, Int](reducerDecimator)(_ + _)

  val reducerRouter = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)), "reducer-router")

  def receive = {
    case EndOfData =>
      reducerRouter ! Broadcast(GetAggregator)
      reducerRouter ! Broadcast(Forward(EndOfData))
    case x: Any => reducerRouter ! x
  }
}

case class SingleFileReader(filename: String)

case class MultipleFileReaders(filename: String)

case object HammerdownProtocol

case object EndOfData

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}

object PrintResults {
  def apply(finalAggregate: Map[String, Int]) = {
    println("FINAL RESULTS")
    finalAggregate.toList sortBy (-_._2) take 20 foreach {
      case (s, i) => println(f"$s%8s:$i%5d")
    }
  }
}
