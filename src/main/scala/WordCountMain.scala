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

object WordCountMain extends App {
  val system = ActorSystem("akka-wordcount")
  val wcSupAct = system.actorOf(Props(new WordCountSupervisor(4, 4)), "wc-super")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    wcSupAct !("chunky", args(0))
    //    wcSupAct !("direct", args(0))
  }
  system.scheduler.schedule(0.seconds, 1.second, wcSupAct, Progress)
}


class MapperCast(outputPath: String, nMappers: Int) extends Actor {
  val output = context.actorSelection(outputPath)

  def funnel = Funnel(outputPath, nMappers)

  def myMapper = Mapper(funnel) {
    ss: String => ss.split(raw"\s+").toSeq
      .map(_.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map(KeyVal(_, 1))
  }

  val router = context.actorOf(BalancingPool(nMappers).props(Props(myMapper)))

  def receive = {
    case x => router ! x
  }
}


class ReducerCast(outputPath: String, nReducers: Int) extends Actor {
  val output = context.actorSelection(outputPath)


  def funnel = Funnel(output, nReducers)
  def myReducer = Reducer[String, Int](funnel)(_ + _)
  val router = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)))



  def receive = {
    case x: Any => router ! x
    case EndOfData =>
      router ! Broadcast(GetAggregator)
      router ! Broadcast(EndOfData)
      //reducerRouter ! Broadcast(PoisonPill)
  }
}

case object Progress

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {

  val mapperCast = context.actorOf(Props(new MapperCast("/user/super/reducer-cast", nMappers)))
  val reducerCast = context.actorOf(Props(new ReducerCast("/user/super", nReducers)), "reducer-cast")

  var finalAggregate: Map[String, Int] = Map()
  var progress = 0L

  def receive = {
    case ("chunky", filename: String) =>
      val fileSize = new File(filename).length.toInt
      val chunkSize = (fileSize + nMappers) / nMappers
      ChunkLimits(fileSize, chunkSize) foreach {
        mapperCast ! FileChunkLineReader(filename)(_).iterator
      }
      mapperCast ! Broadcast(EndOfData)
      //mapperCast ! Broadcast(PoisonPill)

    case ("direct", filename: String) =>
      val lineItr = Source.fromFile(filename).getLines()
      lineItr foreach (mapperCast ! _)
      mapperCast ! Broadcast(EndOfData)
      //mapper ! Broadcast(PoisonPill)


    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag

      // Debug messages to demonstrate the disjoint key sets from the reducer actors.
      println(sender())
      ag.toList.sortBy(-_._2).take(5) foreach print
      println()

    case EndOfData =>
      println("FINAL RESULTS")
      finalAggregate.toList sortBy (-_._2) take 100 foreach { case (s, i) => print(s + " ") }
      println(progress)

    case DataAck(l) => progress += l

    case Progress => println(progress)
  }
}

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}
