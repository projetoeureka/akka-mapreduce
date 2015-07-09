package demo

import akka.actor._
import akka.routing.{Broadcast, ConsistentHashingPool, SmallestMailboxPool}
import geekie.mapred._
import geekie.mapred.io.{FileChunks, FileSize}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

/**
 * Created by nlw on 12/04/15.
 * A simplistic Map/Reduce with lousy job flow control.
 *
 */
object SimplisticWordCountMain extends App {
  println("RUNNING SIMPLISTIC M/R")
  val system = ActorSystem("akka-wordcount")
  val wordcountSupervisor = system.actorOf(Props(classOf[SimplisticWordCountSupervisor], 8, 8), "supervisor")
  if (args.length < 1) println("MISSING INPUT FILENAME")
  else {
    //wordcountSupervisor ! MultipleFileReaders(args(0))
    wordcountSupervisor ! SimplisticWordCountSupervisor.SingleFileReader(args(0))
  }
}

class SimplisticWordCountSupervisor(nMappers: Int, nReducers: Int) extends Actor {

  def myReducer = ReducerTask[String, Int](self)(_ + _)

  val reducer = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)), "reducer")

  def myMapper = MapperTask(reducer) {
    ss: String => ss.split(raw"\s+").toSeq
      .map(_.trim.toLowerCase.filterNot(_ == ','))
      .filterNot(StopWords.contains)
      .map { ww => KeyVal(ww, 1) }
  }

  val mapper = context.actorOf(SmallestMailboxPool(nMappers).props(Props(myMapper)), "mapper")

  context.watch(mapper)
  context.watch(reducer)

  var progress = 0
  var finalAggregate: Map[String, Int] = Map()

  def receive = {
    case SimplisticWordCountSupervisor.SingleFileReader(filename) =>
      println(s"PROCESSING FILE $filename")
      Source.fromFile(filename).getLines() foreach (mapper ! _)
      mapper ! Broadcast(PoisonPill)

    case Terminated(`mapper`) =>
      reducer ! Broadcast(GetAggregator)
      reducer ! Broadcast(PoisonPill)

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag

    case Terminated(`reducer`) =>
      PrintWordcountResults(finalAggregate)
      context.system.scheduler.scheduleOnce(1.second, self, SimplisticWordCountSupervisor.HammerdownProtocol)

    case SimplisticWordCountSupervisor.HammerdownProtocol => context.system.shutdown()
  }
}

object SimplisticWordCountSupervisor {

  case object HammerdownProtocol

  case class SingleFileReader(filename: String)

}





