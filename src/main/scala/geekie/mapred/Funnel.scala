package geekie.mapred

import akka.actor.{ActorRef, PoisonPill, Actor}
import akka.routing.Broadcast

/**
 * Created by nlw on 10/04/15.
 * A data funnel that waits until the last transmitter stops transmission to pass the end marker.
 *
 */

case object EndOfData

class Funnel(dest: ActorRef, totalTransmitters: Int) extends Actor {
  private var nTransmitters = totalTransmitters

  def receive = {
    case EndOfData =>
      nTransmitters -= 1
      if (nTransmitters == 0) {
        dest ! EndOfData
      }
    case x: Any => dest.tell(x, sender())
  }
}

object Funnel {
  def apply(destPath: String, totalTransmitters: Int) = new Funnel(destPath, totalTransmitters)
}


