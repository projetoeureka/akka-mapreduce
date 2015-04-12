/**
 * Created by nlw on 08/04/15.
 * A mapper that takes an object (iterator) as input and forwards zero or more corresponding to the reducers.
 *
 */
package geekie.mapred

import akka.actor.{ActorRef, Actor}

import scala.reflect.ClassTag

case class Forward[T](obj: T)

class Mapper[A: ClassTag, B](output: ActorRef, f: A => Seq[B]) extends Actor {

  def receive = {
    case s: A =>
      sender ! DataAck(s.asInstanceOf[String].getBytes.length + 1)
      f(s) foreach (output ! _)

    case strItr: Iterator[A] =>
      strItr flatMap {
        s: A =>
          sender ! DataAck(s.asInstanceOf[String].getBytes.length + 1)
          f(s)
      } foreach (output ! _)

    case Forward(x) =>
      output ! x
  }
}

object Mapper {
  def apply[A: ClassTag, B](output: ActorRef)(f: A => Seq[B]) = new Mapper(output, f)
}

case class DataAck(length: Int)