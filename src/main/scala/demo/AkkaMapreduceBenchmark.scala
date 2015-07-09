package demo

import akka.actor._
import geekie.mapred.PipelineHelpers._
import geekie.mapred._
import geekie.mapred.io.DataChunk

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Runs a very simple counting job.
 *
 */
class AkkaMapreduceBenchmark extends Actor {

  println("RUNNING AKKA MAPREDUCE BENCHMARK")

  type RedK = String
  type RedV = Int

  val nMappers = System.getProperty("mappers", "4").toInt
  val nReducers = System.getProperty("reducers", "4").toInt
  val nKeys = System.getProperty("num.keys", "10").toInt
  val windowSize = System.getProperty("window", "8").toInt
  val nChunks = System.getProperty("num.chunks", "1000").toInt
  val chunkSize = System.getProperty("chunk.size", "10000").toLong

  val myWorkers = PipelineStart[Long] map { batchSize: Long =>
    (0L until batchSize).iterator map (_ % nKeys) map (KeyVal(_, 1))
  } times nMappers reduce (_ + _) times nReducers output self

  val mapper = myWorkers.head

  var finalAggregate: Map[RedK, RedV] = Map()

  val theData = (0 until nChunks) map (DataChunk(List(chunkSize), _, None))

  val startTime = System.currentTimeMillis()

  val dataSource = context.actorOf(Props(classOf[ChunkScheduler[Int]], mapper, theData, windowSize, Some(nChunks)), "wc-super")

  def receive = working(dataSource)

  def working(dataSource: ActorRef): Receive = {
    case msg: ProgressReport =>
      dataSource forward msg
    case ReducerResult(agAny) =>
      finalAggregate ++= agAny.asInstanceOf[Map[RedK, RedV]]
    case EndOfData =>
      PrintWordcountResults(finalAggregate)
      context.system.scheduler.scheduleOnce(2.second, self, PoisonPill)
      val totalTime = (System.currentTimeMillis() - startTime) / 1e3
      println(f"TOTAL TIME: $totalTime%.2fs")
  }
}