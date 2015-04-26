package geekie.mapred

import akka.actor.{Actor, ActorRef}

import scala.collection.mutable.{Map => MutableMap}

/**
 * Created by nlw on 10/04/15.
 * Transmits only 1 in every `factor` messages of a certain kind that arrive. Anything else is forwarded directly.
 *
 */
class Decimator(output: ActorRef, factor: Int) extends Actor {
  val phase = MutableMap[Decimable, Int]().withDefaultValue(0)

  def receive = {
    case msg: Decimable =>
      phase(msg) = if (phase(msg) >= factor - 1) 0 else phase(msg) + 1
      if (phase(msg) == 0) output forward msg

    case x: Any => output forward x
  }
}

object Decimator {
  def apply(output: ActorRef, factor: Int, obj: Any) = new Decimator(output, factor)
}
