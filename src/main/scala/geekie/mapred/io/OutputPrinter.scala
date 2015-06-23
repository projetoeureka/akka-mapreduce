package geekie.mapred.io

import akka.actor.Actor

class OutputPrinter extends Actor {
  def receive = {
    case (k: String, v: String) =>
      println(f"$k\t$v")
    case s: String =>
      println(s)
  }
}
