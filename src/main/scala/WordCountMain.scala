/**
 * Created by nlw on 05/04/15. Akka based map-reduce task example.
 *
 */

import akka.actor._
import akka.dispatch.sysmsg.Terminate
import akka.routing._
import geekie.mapred.io.{ChunkLimits, FileChunkLineReader}
import scala.io.Source

import geekie.mapred._

import scala.concurrent.ExecutionContext.Implicits.global


object WordCountMain extends App {
  val system = ActorSystem("akka-wordcount")
  val wcSupAct = system.actorOf(Props(new WordCountSupervisor(4, 4)), "wordcount-supervisor")
  wcSupAct ! args(0)
}

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {
  //  val wcMapAct = context.actorOf(BalancingPool(nMappers).props(Props[WordCountMapper]),
  val wcMapAct = context.actorOf(BalancingPool(nMappers).props(Props[WordCountItrMapper]), "wordcount-mapper-router")
  val wcRedAct = context.actorOf(ConsistentHashingPool(nReducers).props(Props[WordCountReducer]), "wordcount-reducer-router")

  context.watch(wcMapAct)
  context.watch(wcRedAct)

  var finishedMappers = 0
  var finishedReducers = 0

  var finalAggregate: Map[String, Int] = Map()

  def receive = {
    case filename: String =>
      /*
            val fileSize = Source.fromFile(filename).length
            for ((ia, ib) <- ChunkLimits(fileSize, fileSize / 8))
              wcMapAct ! FileChunkLineReader(filename, ia, ib)
            wcMapAct ! Broadcast(MapperFinish)
      */

      val lineItr = Source.fromFile(filename).getLines()
      lineItr foreach (wcMapAct ! _)
      wcMapAct ! Broadcast(PoisonPill)

    case Terminated(`wcMapAct`) =>
      wcRedAct ! Broadcast(GetAggregator)
      wcRedAct ! Broadcast(PoisonPill)
    case Terminated(`wcRedAct`) =>
      println("FINAL RESULTS")
      finalAggregate.toList.sortBy(-_._2).take(100).foreach({ case (s, i) => print(s + " ") })
    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag
      // Debug messages to demonstrate the disjoint key sets from the reducer actors.
      println(sender())
      ag.toList.sortBy(-_._2).take(5) foreach print
      println()

  }
}

/*
class WordCountMapper extends StringMultiMapper("../../wordcount-reducer-router")(
  _.split(raw"\s+").map(_.trim.toLowerCase.filterNot(_ == ',')).filterNot(StopWords.contains).map(KeyVal(_, 1))
)
*/

class WordCountItrMapper extends Mapper[KeyVal[String, Int]]("../../wordcount-reducer-router")({
  ss: String => ss.split(raw"\s+").map(_.trim.toLowerCase.filterNot(_ == ',')).filterNot(StopWords.contains).map(KeyVal(_, 1))
})

class WordCountReducer extends Reducer[String, Int](_ + _)

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}
