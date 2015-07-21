package geekie.mapreddemo

import java.io.File

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import geekie.mapred.Mapred
import geekie.mapred.MapredWorker.KeyVal

class WordcountSupervisor extends Actor {

  println("RUNNING NEAT M/R")

  implicit val system = context.system
  implicit val materializer = ActorMaterializer()

  import akka.stream.io.Implicits._

  def propertyOrDefault(propertyName: String, default: Int) =
    sys.props.get(propertyName) map (_.toInt) getOrElse default

  val nWorkers = propertyOrDefault("workers", 4)
  val chunkSize = propertyOrDefault("chunk-size", 5000)

  val filename = sys.props("filename")
  val file = new File(filename)

  val stopWords = scala.io.Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def mapFun(line: ByteString) = for {
    word <- ("""[^\p{L}\p{Mc}]+""".r split line.utf8String).iterator
    lower = word.trim.toLowerCase
    if !(stopWords contains lower)
  } yield KeyVal(lower, 1)

  val mapredProps = Mapred.props(nWorkers)(mapFun)(_ + _) { counters =>
    println("FINAL RESULTS")
    counters.toList sortBy (-_._2) take 20 foreach {
      case (s, i) => println(f"$s%8s:${i.toInt}%5d")
    }
    self ! PoisonPill
  }

  Source.synchronousFile(file)
    .via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 2048, allowTruncation = true))
    .grouped(chunkSize)
    .to(Sink.actorSubscriber(mapredProps))
    .run()

  override def receive = {
    case _ =>
  }
}
