import java.io.File

import akka.actor._
import akka.routing.{ConsistentHashingPool, SmallestMailboxPool, Broadcast}
import scala.io.Source
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import geekie.mapred._
import geekie.mapred.io.{FileChunks, FileSize, FileChunkLineReader, ChunkLimits}


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
    wordcountSupervisor ! MultipleFileReaders(args(0))
    // wcSupAct ! SingleFileReader(args(0))
  }
  system.scheduler.schedule(0.seconds, 1.second, wordcountSupervisor, Progress)
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
  var fileSize = 0
  var finalAggregate: Map[String, Int] = Map()

  def receive = {

    case MultipleFileReaders(filename) =>
      fileSize = FileSize(filename)
      FileChunks(filename, 100*nMappers) foreach (mapper ! _.iterator)
      mapper ! Broadcast(PoisonPill)

    case SingleFileReader(filename) =>
      fileSize = FileSize(filename)
      Source.fromFile(filename).getLines() foreach (mapper ! _)
      mapper ! Broadcast(PoisonPill)

    case Terminated(`mapper`) =>
      reducer ! Broadcast(GetAggregator)
      reducer ! Broadcast(PoisonPill)

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[String, Int]]
      finalAggregate = finalAggregate ++ ag

    case Terminated(`reducer`) =>
      println("FINAL RESULTS")
      finalAggregate.toList sortBy (-_._2) take 100 foreach { case (s, i) => print(s + " -> " + i + " ") }
      context.system.scheduler.scheduleOnce(1.second, self, HammerdownProtocol)

    case HammerdownProtocol => context.system.shutdown()

    case DataAck(l) => progress += l

    case Progress => println(f"${(100.0 * progress) / fileSize}%6.1f%% - $progress / $fileSize")
  }
}
