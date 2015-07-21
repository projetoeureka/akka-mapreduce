package geekie.mapreddemo

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import geekie.mapred.Mapred
import geekie.mapred.MapredWorker.KeyVal

class AkkaMapreduceBenchmark extends Actor {

  println("RUNNING BENCHMARK")

  implicit val system = context.system
  implicit val materializer = ActorMaterializer()

  def propertyOrDefault(propertyName: String, default: Int) =
    sys.props.get(propertyName) map (_.toInt) getOrElse default

  val nWorkers = propertyOrDefault("workers", 4)
  val chunkSize = propertyOrDefault("chunk.size.max", 100)

  val nKeys = propertyOrDefault("app.keys", 10)
  val nTasks = propertyOrDefault("app.tasks", 10000)
  val taskSize = propertyOrDefault("app.tasks.size", 10000)

  def mapFun(len: Long) = (0L until len).iterator map (_ % nKeys) map (KeyVal(_, 1))

  val mapredProps = Mapred.props(nWorkers)(mapFun)(_ + _) { counters =>
    println(counters)
    self ! PoisonPill
  }

  Source(for (_ <- 1 to nTasks) yield taskSize.toLong)
    .grouped(chunkSize)
    .to(Sink.actorSubscriber(mapredProps))
    .run()

  override def receive = {
    case _ =>
  }
}
