import akka.actor._
import akka.routing._
import geekie.mapred._
import geekie.mapred.io.{FileChunks, FileSize, ChunkLimits, FileChunkLineReader}

import scala.io.Source
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by nlw on 05/04/15. Akka based map-reduce task example.
 *
 */
object WordCountMain extends App {
  println("RUNNING NEAT M/R")
  val system = ActorSystem("akka-wordcount")
  val wcSupAct = system.actorOf(Props(classOf[WordCountSupervisor], 8, 8), "wc-super")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    wcSupAct ! MultipleFileReaders(args(0))
    // wcSupAct ! SingleFileReader(args(0))
  }
  system.scheduler.schedule(0.seconds, 1.second, wcSupAct, Progress)
}

case class SingleFileReader(filename: String)

case class MultipleFileReaders(filename: String)

case object HammerdownProtocol

case object EndOfData

case object Progress

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {

  val reducer = context.actorOf(Props(classOf[Reducer], self, nReducers), "reducer")
  val mapper = context.actorOf(Props(classOf[Mapper], reducer, nMappers), "mapper")

  var progress = 0
  var fileSize = 0
  var finalAggregate: Map[String, Int] = Map()

  def receive = {
    case MultipleFileReaders(filename) =>
      fileSize = FileSize(filename)
      FileChunks(filename, 10 * nMappers) foreach (chunk => mapper ! VerboseMapTask(chunk.iterator))
      mapper ! EndOfData

    case SingleFileReader(filename) =>
      fileSize = FileSize(filename)
      Source.fromFile(filename).getLines() foreach (mapper ! _)
      mapper ! EndOfData

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag

    case EndOfData =>
      println("FINAL RESULTS")
      finalAggregate.toList sortBy (-_._2) take 100 foreach { case (s, i) => print(s + " -> " + i + " ") }
      context.system.scheduler.scheduleOnce(1.second, self, HammerdownProtocol)

    case HammerdownProtocol => context.system.shutdown()

    case DataAck(l) => progress += l

    case Progress => println(f"${(100.0 * progress) / fileSize}%6.1f%% - $progress / $fileSize")
  }
}

class Mapper(output: ActorRef, nMappers: Int) extends Actor {

  val mapperDec = context.actorOf(Props(classOf[Decimator], output, nMappers, EndOfData), "mapper-decimator")

  def procStr = {
    ss: String => ss.split(raw"\s+").toSeq
      .map(_.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map { ww => KeyVal(ww, 1) }
  }

  def myMapper = MapperTask(mapperDec)(procStr)

  val mapperRouter = context.actorOf(SmallestMailboxPool(nMappers).props(Props(myMapper)), "mapper-router")

  def receive = {
    case EndOfData => mapperRouter ! Broadcast(Forward(EndOfData))
    case x: Any => mapperRouter.tell(x, sender())
  }
}


class Reducer(output: ActorRef, nReducers: Int) extends Actor {

  val reducerDec = context.actorOf(Props(classOf[Decimator], output, nReducers, EndOfData), "reducer-decimator")

  def myReducer = ReducerTask[String, Int](reducerDec)(_ + _)

  val reducerRouter = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)), "reducer-router")

  def receive = {
    case EndOfData =>
      reducerRouter ! Broadcast(GetAggregator)
      reducerRouter ! Broadcast(Forward(EndOfData))
    case x: Any => reducerRouter.tell(x, sender())
  }
}

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}
