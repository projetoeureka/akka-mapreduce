package geekie.mapred

import akka.actor.{Actor, ActorRef}

import scala.reflect.ClassTag

/**
 * Created by nlw on 10/04/15.
 * A data funnel that waits until the last transmitter stops transmission to pass the end marker.
 *
 */

// case object EndOfData

class Decimator(dest: ActorRef, factor: Int, targetObject: Any) extends Actor {
  var phase = 0

  def receive = {
    case `targetObject` =>
      phase = if (phase <= 0) factor - 1 else phase - 1
      if (phase <= 0) dest ! targetObject

    case x: Any => dest ! x
  }
}

object Decimator {
  def apply(output: ActorRef, factor: Int, obj: Any) = new Decimator(output, factor, obj)
}
