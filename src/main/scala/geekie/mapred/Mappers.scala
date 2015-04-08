/**
 * Created by nlw on 08/04/15. Mappers!
 *
 */
package geekie.mapred

import akka.actor.Actor
import geekie.mapred.io.{FileChunk, FileChunkLineReader}


case object MapperFinish

// A mapper that takes a string as input and outputs zero or more objects that are forwarded to the reducers.
class StringMultiMapper[B](reducerPath: String)(f: String => Seq[B]) extends Actor {
  val reducer = context.actorSelection(reducerPath)

  def receive = {
    case s: String => f(s) foreach (reducer ! _)
    case MapperFinish =>
      sender ! MapperFinish
      context.stop(self)
  }
}

class StringIteratorMultiMapper[B](reducerPath: String)(f: String => Seq[B]) extends Actor {
  val reducer = context.actorSelection(reducerPath)

  def receive = {
    case strItr: Iterator[String] =>
      strItr.flatMap(f).foreach(reducer ! _)
    case MapperFinish =>
      sender ! MapperFinish
      context.stop(self)
  }
}
