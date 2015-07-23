package geekie.mapreddemo

import akka.actor.Actor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import geekie.mapred._

class SimpleExample extends Actor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()

  val theData = "Antidisestablishmentarianism".toList

  val nWorkers = 4
  val chunkSize = theData.size / nWorkers

  def mapFun(ch: Char) = Some(MapredWorker.KeyVal(ch.toLower, 1))

  val mapredProps = Mapred.props(nWorkers)(mapFun)(_ + _) { counters =>
    println(s"FINAL RESULTS\n$counters")
    context stop self
  }

  Source(theData).grouped(chunkSize).to(Sink.actorSubscriber(mapredProps)).run()

  override def receive = {
    case _ =>
  }
}
