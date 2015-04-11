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
  val wcSupAct = system.actorOf(Props(new WordCountSupervisor(4, 4)), "wc-super")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    wcSupAct !("chunky", args(0))
    // wcSupAct !("direct", args(0))
  }
  system.scheduler.schedule(0.seconds, 1.second, wcSupAct, Progress)
}

case object Progress

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {

  val reducerCast = context.actorOf(Props(new ReducerCast(self, nReducers)), "reducer-cast")
  val mapperCast = context.actorOf(Props(new MapperCast(reducerCast, nMappers)), "mapper-cast")

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
      mapperCast ! Broadcast(EndOfData)
      mapperCast ! Broadcast(PoisonPill)

    case ("direct", filename: String) =>
      setFileSize(filename)
      val lineItr = Source.fromFile(filename).getLines()
      lineItr foreach (mapperCast ! _)
      mapperCast ! Broadcast(EndOfData)
      mapperCast ! Broadcast(PoisonPill)

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag

    case EndOfData =>
      println("FINAL RESULTS")
      finalAggregate.toList sortBy (-_._2) take 100 foreach { case (s, i) => print(s + " ") }
      println(progress)
      context.system.shutdown()

    case DataAck(l) => progress += l

    case Progress => println(f"${(100.0 * progress) / fileSize}%6.1f%% - $progress / $fileSize")
  }
}

class MapperCast(output: ActorRef, nMappers: Int) extends Actor {
  //val output = context.actorSelection(outputPath)

  val funnel = context.actorOf(Props(new Funnel(output, nMappers)), "yaya")

  def myMapper = Mapper(funnel) {
    ss: String => ss.split(raw"\s+").toSeq
      .map(_.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map(KeyVal(_, 1))
  }

  val router = context.actorOf(BalancingPool(nMappers).props(Props(myMapper)), "yoyo")

  def receive = {
    case x: Any => router.tell(x, sender())
  }
}


class ReducerCast(output: ActorRef, nReducers: Int) extends Actor {
  //val output = context.actorSelection(outputPath)

  val funnel = context.actorOf(Props(Funnel(output, nReducers)), "yayaay")

  def myReducer = Reducer[String, Int](funnel)(_ + _)

  val router = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)), "routA")


  def receive = {
    case EndOfData =>
      router ! Broadcast(GetAggregator)
      router ! Broadcast(EndOfData)
    case x: Any => router ! x
  }
}

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}
