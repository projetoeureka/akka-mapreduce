package geekie.mapreddemo

import akka.actor._
import geekie.mapred.PipelineHelpers._
import geekie.mapred._
import geekie.mapred.io.DataChunk
import geekie.mapred.utils.Counter

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * Runs a very simple counting job.
 *
 */
class AkkaMapreduceBenchmark extends Actor {

  println("RUNNING AKKA MAPREDUCE BENCHMARK")

  type RedK = Int
  type RedV = Long

  def propertyOrDefault(propertyName: String, default: Int) = sys.props.get(propertyName) map (_.toInt) getOrElse default

  val nMappers = propertyOrDefault("mappers", 4)
  val nReducers = propertyOrDefault("reducers", 4)
  val nChunks = propertyOrDefault("chunks", nMappers * 4)
  val chunkWindow = propertyOrDefault("reducers", nMappers * 2)
  val chunkSizeMax = sys.props.get("chunk.size.max") map (_.toLong)
  val nKeys = propertyOrDefault("keys", 10)

  def sq(x: Long) = x * x

  val myWorkers = PipelineStart[Long] map { batchSize: Long =>
    (0L until batchSize).iterator map (sq(_) % nKeys) map (KeyVal(_, 1))
  } lazymap true times nMappers reduce (_ + _) times nReducers output self

  val mapper = myWorkers.head

  var finalAggregate = Counter(Map[RedK, RedV](), (a: RedV, b: RedV) => a + b)

  val theData = (0 until nChunks) map (DataChunk(List(chunkSizeMax.get), _, None))

  val dataSource = context.actorOf(Props(classOf[DataChunkSource[Int]], mapper, theData, chunkWindow, Some(nChunks)), "wc-super")

  val startTime = System.currentTimeMillis()

  def receive = working(dataSource)

  def working(dataSource: ActorRef): Receive = {
    case msg: ProgressReport =>
      dataSource forward msg
    case ReducerResult(agAny) =>
      finalAggregate = finalAggregate addFromMap agAny.asInstanceOf[Map[RedK, RedV]]
    case EndOfData =>
      PrintWordcountResults(finalAggregate.counter)
      context.system.scheduler.scheduleOnce(2.second, self, PoisonPill)
      val totalTime = (System.currentTimeMillis() - startTime) / 1e3
      println(f"TOTAL COUNT: ${finalAggregate.counter.values.sum}")
      println(f"TOTAL TIME: $totalTime%.2fs")
  }
}
