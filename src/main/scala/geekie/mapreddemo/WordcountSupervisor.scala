package geekie.mapreddemo

import java.io.File

import akka.actor._
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import geekie.mapred.Mapred
import geekie.mapred.MapredWorker.KeyVal

import scala.concurrent.ExecutionContext.Implicits.global

class WordcountSupervisor extends Actor {

  println("RUNNING NEAT M/R")

  // actor system and implicit materializer
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()

  // read lines from a log file
  val logFile = new File("/home/nic/machado.txt")

  val nWorkers = 4

  val stopWords = scala.io.Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  import akka.stream.io.Implicits._

  def getWords(ss: String) = {
    for {
      word <- ("""[.,\-\s]+""".r split ss).iterator
      lower = word.trim.toLowerCase
      if !(stopWords contains lower)
    } yield lower
  }

  def mapp(s: String) = Some(KeyVal(s, 1))
  def redd(a: Int, b: Int): Int = a + b

  val mapredProps = Mapred.props(nWorkers)(mapp)(redd)

  val res = Source.synchronousFile(logFile)
    .via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 8192, allowTruncation = true))
    .map(_.utf8String)
    .map(ss => getWords(ss).toVector)
    .to(Sink.actorSubscriber(mapredProps)).run()

  res onSuccess { case _ => println("akkabou") }

  def receive = {
    case AkkaStreamTest.Hammerdown =>
      println("END THIS")
      context.system.shutdown()
  }
}

object AkkaStreamTest {
  case object Hammerdown
}

