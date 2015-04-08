/**
 * Created by nlw on 08/04/15.
 * A mapper that takes an object (iterator) as input and forwards zero or more corresponding to the reducers.
 *
 */
package geekie.mapred

import akka.actor.Actor

import scala.reflect.ClassTag

class Mapper[A: ClassTag, B](reducerPath: String, f: A => Seq[B]) extends Actor {
  val reducer = context.actorSelection(reducerPath)

  def receive = {
    case s: A => f(s) foreach (reducer ! _)
    case strItr: Iterator[A] => strItr flatMap f foreach (reducer ! _)
  }
}

object Mapper {
  def apply[A: ClassTag, B](reducerPath: String)(f: A => Seq[B]) = new Mapper(reducerPath, f)
}