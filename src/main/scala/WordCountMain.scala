import akka.actor._
import akka.routing._
import scala.io.Source

import geekie.mapred._

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Created by nlw on 05/04/15. Akka based map-reduce task example.
 */

object WordCountMain extends App {
  val system = ActorSystem("akka-wordcount")
  val wcSupAct = system.actorOf(Props(new WordCountSupervisor(8, 8)), "wordcount-supervisor")
  wcSupAct ! args(0)
}

class WordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {
  val wcMapAct = context.actorOf(BalancingPool(nMappers).props(Props[WordCountMapper]),
    "wordcount-mapper-router")
  val wcRedAct = context.actorOf(ConsistentHashingPool(nReducers).props(Props[WordCountReducer]),
    "wordcount-reducer-router")

  var finishedMappers = 0
  var finishedReducers = 0

  var finalAggregate: Map[String, Int] = Map()
  
  def receive = {
    case filename: String => {
      val lineItr = Source.fromFile(filename).getLines()
      lineItr foreach (wcMapAct ! _)
      wcMapAct ! Broadcast(MapperFinish)
    }
    case MapperFinish => {
      finishedMappers += 1
      if (finishedMappers == nMappers) {
        wcRedAct ! Broadcast(ReducerFinish)
      }
    }
    case ReducerResult(agAny) => {
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finishedReducers += 1

      finalAggregate = finalAggregate ++ ag
      // Debug messages to demonstrate the disjoint key sets from the reducer actors.
      println(sender)
      ag.toList.sortBy(-_._2).take(5) foreach print
      println()

      if (finishedReducers == nReducers) {
        println("FINAL RESULTS")
        finalAggregate.toList.sortBy(-_._2).take(100).foreach({ case (s, i) => print(s + " ") })
      }
    }
  }
}

class WordCountMapper extends StringMultiMapper("../../wordcount-reducer-router")(
  _.split(raw"\s+").map(_.trim.toLowerCase.filterNot(_ == ',')).filterNot(StopWords.contains).map(KeyVal(_, 1))
)

class WordCountReducer extends Reducer[String, Int](_ + _)

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}
