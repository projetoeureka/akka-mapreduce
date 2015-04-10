package geekie.mapred

import akka.actor.{PoisonPill, Actor}
import akka.routing.Broadcast

/**
 * Created by nlw on 10/04/15.
 * A data funnel that waits until the last transmitter stops transmission to pass the end marker.
 *
 */

case object Obituary

class Funnel(reducerPath: String, totalTransmitters: Int) extends Actor {
  val reducer = context.actorSelection(reducerPath)
  private var nTransmitters = totalTransmitters

  def receive = {
    case Obituary =>
      nTransmitters -= 1
      if (nTransmitters == 0) {
        reducer ! Obituary
        nTransmitters = totalTransmitters
      }
    case x: Any => reducer.tell(x, sender())
  }
}

object Funnel {
  def apply(destPath: String, totalTransmitters: Int) = new Funnel(destPath, totalTransmitters)
}
