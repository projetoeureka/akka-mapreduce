import akka.actor._
import geekie.mapred._
import geekie.mapred.io.FileChunks

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.reflect.ClassTag

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

    // def mySup = MapReduceSupervisor[String,String,Int](4,4) {ss: String=>Seq(KeyVal("oi",1))} (_ + _)

    def mySup = MapReduceSupervisor[String,String,Int](4,4) { ss: String =>
      ss.split(raw"\s+").toSeq
        .map(_.trim.toLowerCase.filterNot(_ == ','))
        .filterNot(StopWords.contains)
        .map { ww => KeyVal(ww, 1) }
    } (_ + _)

    val wordcountSupervisor = system.actorOf(Props(mySup), "wc-super")
    wordcountSupervisor ! MultipleFileReaders(args(0))
    // wcSupAct ! SingleFileReader(args(0))
  }
}

class MapReduceSupervisor[A:ClassTag, RedK:ClassTag, RedV:ClassTag](nMappers: Int, nReducers: Int)
                                        (mapFun: A=>Seq[KeyVal[RedK, RedV]])
                                        (redFun: (RedV,RedV)=>RedV) extends Actor
{
  val reducer = Reducer[RedK, RedV](self, nReducers) (redFun)
  val mapper = Mapper[A, KeyVal[RedK, RedV]](reducer, nMappers)(mapFun)

  var progress = 0
  var finalAggregate: Map[RedK, RedV] = Map()

  def receive = {
    case MultipleFileReaders(filename) =>
      println(s"PROCESSING FILE $filename")
      FileChunks(filename, nMappers) foreach (mapper ! _.iterator)
      mapper ! EndOfData

    case ReducerResult(agAny) =>
      val ag = agAny.asInstanceOf[Map[RedK, RedV]]
      finalAggregate = finalAggregate ++ ag

    case EndOfData =>
      PrintResults(finalAggregate)
      context.system.scheduler.scheduleOnce(1.second, self, HammerdownProtocol)

    case HammerdownProtocol => context.system.shutdown()
  }
}

object MapReduceSupervisor {
  def apply[A:ClassTag, RedK:ClassTag, RedV:ClassTag](nm: Int, nr:Int)(mapFun: A=>Seq[KeyVal[RedK, RedV]])
                                                     (redFun: (RedV,RedV)=>RedV) = new MapReduceSupervisor(nm, nr)(mapFun)(redFun)
}

case class SingleFileReader(filename: String)

case class MultipleFileReaders(filename: String)

case object HammerdownProtocol

object StopWords {
  private val stopwords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopwords.contains(s)
}

object PrintResults {
  def apply[RedK, RedV](finalAggregate: Map[RedK, RedV]) = {
    println("FINAL RESULTS")
    val ag = finalAggregate.asInstanceOf[Map[String, Int]]
    ag.toList sortBy (-_._2) take 20 foreach {
      case (s, i) => println(f"$s%8s:$i%5d")
    }
  }
}
