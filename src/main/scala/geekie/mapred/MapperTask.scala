/**
 * Created by nlw on 08/04/15.
 * A mapper that takes an object (iterator) as input and forwards zero or more corresponding to the reducers.
 *
 */
package geekie.mapred

import akka.actor.{ActorRef, Actor}

import scala.reflect.ClassTag

case class Forward[T](obj: T)

case class VerboseMapTask[A: ClassTag](task: Iterator[A])

class MapperTask[A: ClassTag, B](output: ActorRef, f: A => Seq[B]) extends Actor {

  def receive = {
    case s: A => f(s) foreach (output ! _)

    case strItr: Iterator[A] => strItr flatMap f foreach (output ! _)

    case VerboseMapTask(strItr: Iterator[A]) =>
      var byteCount = 0
      strItr foreach { s: A =>
        byteCount += s.asInstanceOf[String].getBytes.length + 1
        f(s) foreach (output ! _)
      }
      sender ! DataAck(byteCount)

    case Forward(x) =>
      output ! x
  }
}

object MapperTask {
  def apply[A: ClassTag, B](output: ActorRef)(f: A => Seq[B]) = new MapperTask(output, f)
}

case class DataAck(length: Int)