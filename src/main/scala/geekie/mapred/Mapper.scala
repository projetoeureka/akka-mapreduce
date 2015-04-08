/**
 * Created by nlw on 08/04/15. Mappers!
 *
 */
package geekie.mapred

import akka.actor.Actor
import geekie.mapred.io.{FileChunk, FileChunkLineReader}

// A mapper that takes an object (iterator) as input and forwards zero or more corresponding to the reducers.
class Mapper[B](reducerPath: String)(f: String => Seq[B]) extends Actor {
  val reducer = context.actorSelection(reducerPath)

  def receive = {
    case s: String => f(s) foreach (reducer ! _)
    case strItr: Iterator[String] =>
      strItr.flatMap(f).foreach(reducer ! _)
  }
}
