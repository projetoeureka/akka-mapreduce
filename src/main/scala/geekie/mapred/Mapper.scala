/**
 * Created by nlw on 08/04/15.
 * A mapper that takes an object (iterator) as input and forwards zero or more corresponding to the reducers.
 *
 */
package geekie.mapred

import akka.actor.{ActorRef, Actor}

import scala.reflect.ClassTag

class Mapper[A: ClassTag, B](output: ActorRef, f: A => Seq[B]) extends Actor {

  def receive = {
    case s: A =>
      sender ! DataAck(s.asInstanceOf[String].length)
      f(s) foreach (output ! _)

    case strItr: Iterator[A] =>
      strItr flatMap {
        s: A =>
          sender ! DataAck(s.asInstanceOf[String].length)
          f(s)
      } foreach (output ! _)

    case EndOfData => output ! EndOfData

  }
}

object Mapper {
  def apply[A: ClassTag, B](superPath: String)(f: A => Seq[B]) = new Mapper(superPath, f)
}

case class DataAck(length: Int)