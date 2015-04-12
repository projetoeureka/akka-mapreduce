/**
 * Created by nlw on 05/04/15. Akka based map-reduce task example.
 *
 */

import java.io.File

import akka.actor._
import akka.routing._
import geekie.mapred._
import geekie.mapred.io.{ChunkLimits, FileChunkLineReader}

import scala.io.Source
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

object WordCountMain extends App {
  val system = ActorSystem("akka-wordcount")
  val wcSupAct = system.actorOf(Props(classOf[WordCountSupervisor], 4, 4), "wc-super")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    wcSupAct !("chunky", args(0))
    // wcSupAct !("direct", args(0))
  }
  system.scheduler.schedule(0.seconds, 1.second, wcSupAct, Progress)
}

case object Progress

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {

  val reducerCast = context.actorOf(Props(classOf[ReducerCast], self, nReducers), "reducer-cast")
  val mapperCast = context.actorOf(Props(classOf[MapperCast], reducerCast, nMappers), "mapper-cast")

  var finalAggregate: Map[String, Int] = Map()

  var progress = 0
  var fileSize = 0

  def setFileSize(filename: String) = {
    fileSize = new File(filename).length.toInt
  }

  def receive = {
    case ("chunky", filename: String) =>
      setFileSize(filename)
      ChunkLimits(fileSize, nMappers) foreach {
        mapperCast ! FileChunkLineReader(filename)(_).iterator
      }
      mapperCast ! EndOfData

    case ("direct", filename: String) =>
      setFileSize(filename)
      val lineItr = Source.fromFile(filename).getLines()
      lineItr foreach (mapperCast ! _)
      mapperCast ! EndOfData

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag

    case EndOfData =>
      println("FINAL RESULTS")
      finalAggregate.toList sortBy (-_._2) take 100 foreach { case (s, i) => print(s + " -> " + i + " ") }
      context.system.scheduler.scheduleOnce(1.second, self, "DIE")

    case "DIE" => context.system.shutdown()

    case DataAck(l) => progress += l

    case Progress => println(f"${(100.0 * progress) / fileSize}%6.1f%% - $progress / $fileSize")
  }
}

case object EndOfData

class MapperCast(output: ActorRef, nMappers: Int) extends Actor {

  val mapperDec = context.actorOf(Props(classOf[Decimator], output, nMappers, EndOfData), "map-dec")

  def myMapper = Mapper(mapperDec) {
    ss: String => ss.split(raw"\s+").toSeq
      .map(_.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map { ww => KeyVal(ww, 1) }
  }

  val mapperRouter = context.actorOf(SmallestMailboxPool(nMappers).props(Props(myMapper)), "map-rtr")

  def receive = {
    case EndOfData =>
      mapperRouter ! Broadcast(Forward(EndOfData))

    case x: Any =>
      mapperRouter.tell(x, sender())
  }
}


class ReducerCast(output: ActorRef, nReducers: Int) extends Actor {

  val reducerDec = context.actorOf(Props(classOf[Decimator], output, nReducers, EndOfData), "red-dec")

  def myReducer = Reducer[String, Int](reducerDec)(_ + _)

  val reducerRouter = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)), "red-rtr")

  def receive = {
    case EndOfData =>
      reducerRouter ! Broadcast(GetAggregator)
      reducerRouter ! Broadcast(Forward(EndOfData))

    case x: Any =>
      reducerRouter.tell(x, sender())
  }
}

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}
