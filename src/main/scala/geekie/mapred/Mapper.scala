/**
 * Created by nlw on 08/04/15.
 * A mapper that takes an object (iterator) as input and forwards zero or more corresponding to the reducers.
 *
 */
package geekie.mapred

import akka.actor.Actor

import scala.reflect.ClassTag

class Mapper[A: ClassTag, B](superPath: String, f: A => Seq[B]) extends Actor {
  val supervisor = context.actorSelection(superPath)

  def receive = {
    case s: A =>
      sender ! DataAck(s.asInstanceOf[String].length)
      f(s) foreach (supervisor ! _)

    case strItr: Iterator[A] =>
      strItr flatMap {
        s: A =>
          sender ! DataAck(s.asInstanceOf[String].length)
          f(s)
      } foreach (supervisor ! _)

    case Obituary => supervisor ! Obituary

  }
}

object Mapper {
  def apply[A: ClassTag, B](superPath: String)(f: A => Seq[B]) = new Mapper(superPath, f)
}

case class DataAck(length: Int)