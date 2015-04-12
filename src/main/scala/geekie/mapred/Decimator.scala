package geekie.mapred

import akka.actor.{Actor, ActorRef}

/**
 * Created by nlw on 10/04/15.
 * Transmits only 1 in every `factor` messages of a certain kind that arrive. Anything else is forwarded directly.
 *
 */
class Decimator(dest: ActorRef, factor: Int, targetObject: Any) extends Actor {
  var phase = 0

  def receive = {
    case `targetObject` =>
      phase = if (phase == factor - 1) 0 else phase + 1
      if (phase == 0) dest ! targetObject

    case x: Any => dest ! x
  }
}

object Decimator {
  def apply(output: ActorRef, factor: Int, obj: Any) = new Decimator(output, factor, obj)
}
