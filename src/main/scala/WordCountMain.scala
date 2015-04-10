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


object WordCountMain extends App {
  val system = ActorSystem("akka-wordcount")
  val wcSupAct = system.actorOf(Props(new WordCountSupervisor(4, 4)), "wc-super")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    wcSupAct !("chunky", args(0))
    //    wcSupAct !("direct", args(0))
  }
}

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {

  def myMapper = Mapper("../../wc-reducer") {
    ss: String => ss.split(raw"\s+").toSeq
      .map(_.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map(KeyVal(_, 1))
  }

/*
    def myMapper = Mapper("../../wc-reducer") {
      ss: String => Seq(KeyVal("TOTAL", 1))
    }
*/

  def myReducer = Reducer[String, Int](_ + _)

  val wcMapAct = context.actorOf(BalancingPool(nMappers).props(Props(myMapper)), "wc-mapper")
  val wcRedAct = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)), "wc-reducer")

  context.watch(wcMapAct)
  context.watch(wcRedAct)

  var finalAggregate: Map[String, Int] = Map()
  var progress = 0L

  def receive = {
    case ("chunky", filename: String) =>
      val fileSize = new File(filename).length.toInt
      ChunkLimits(fileSize, (fileSize + nMappers) / nMappers) foreach {
        wcMapAct ! FileChunkLineReader(filename)(_).iterator
      }
      wcMapAct ! Broadcast(PoisonPill)

    case ("direct", filename: String) =>
      val lineItr = Source.fromFile(filename).getLines()
      lineItr foreach (wcMapAct ! _)
      wcMapAct ! Broadcast(PoisonPill)

    case Terminated(`wcMapAct`) =>
      wcRedAct ! Broadcast(GetAggregator)
      wcRedAct ! Broadcast(PoisonPill)

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag
      // Debug messages to demonstrate the disjoint key sets from the reducer actors.
      println(sender())
      ag.toList.sortBy(-_._2).take(5) foreach print
      println()

    case Terminated(`wcRedAct`) =>
      println("FINAL RESULTS")
      finalAggregate.toList sortBy (-_._2) take 100 foreach { case (s, i) => print(s + " ") }
      println(progress)

    case DataAck(l) => progress += l
  }
}

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}
