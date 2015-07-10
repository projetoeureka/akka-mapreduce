package geekie.mapreddemo

import akka.actor._
import geekie.mapred.PipelineHelpers._
import geekie.mapred._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

/**
 * Created by nlw on 05/04/15.
 * Akka based map-reduce task example with neat job flow control.
 *
 */
class WordcountSupervisor extends Actor {

  import geekie.mapred.CounterHelpers._

  println("RUNNING NEAT M/R")

  type A = String
  type RedK = String
  type RedV = Int

  val stopWords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def propertyOrDefault(propertyName: String, default: Int) = sys.props.get(propertyName) map (_.toInt) getOrElse default

  val nMappers = propertyOrDefault("mappers", 4)
  val nReducers = propertyOrDefault("reducers", 4)
  val nChunks = propertyOrDefault("chunks", nMappers * 4)
  val chunkWindow = propertyOrDefault("reducers", nMappers * 2)
  val chunkSizeMax = sys.props.get("chunk.size.max") map (_.toInt)

  val myWorkers = PipelineStart[String] map { ss =>
    (ss split raw"[.,\-\s]+")
      .map(word => word.trim.toLowerCase)
      .filterNot(stopWords.contains)
      .map(KeyVal(_, 1))
  } times nMappers reduce (_ + _) times nReducers output self

  val mapper = myWorkers.head

  var finalAggregate: Map[RedK, RedV] = Map()

  val filename = System.getProperty("filename")

  val dataSource = context.actorOf(Props(classOf[FileChunkSource], mapper, filename, nChunks, chunkWindow, chunkSizeMax), "wc-super")

  def receive = working(dataSource)

  def working(dataSource: ActorRef): Receive = {
    case msg: ProgressReport =>
      dataSource forward msg
    case ReducerResult(agAny) =>
      finalAggregate = finalAggregate + agAny.asInstanceOf[Map[RedK, RedV]]
    case EndOfData =>
      PrintWordcountResults(finalAggregate)
      context.system.scheduler.scheduleOnce(2.second, self, PoisonPill)
  }
}
