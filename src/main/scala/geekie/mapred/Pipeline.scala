package geekie.mapred

import akka.actor.{ActorContext, ActorRef}

import scala.reflect.ClassTag

/**
 * Created by nlw on 17/04/15.
 * A pipeline builder
 */
case class Pipeline[A: ClassTag, RedK: ClassTag, RedV: ClassTag](functionSeq: List[MRFunction])(implicit context: ActorContext) {

  def pmap(myFunc: A => Traversable[KeyVal[RedK, RedV]]): Pipeline[A, RedK, RedV] = Pipeline(MapFunction(myFunc) :: functionSeq)

  def preduce(myFunc: (RedV, RedV) => RedV): Pipeline[A, RedK, RedV] = Pipeline(ReduceFunction(myFunc) :: functionSeq)

  def poutput(dest: ActorRef) = generatePipeline(dest)

  def generatePipeline(dest: ActorRef) =
    (List(dest) /: functionSeq) {
      (out, ww) =>
        ww match {
          case MapFunction(f) =>
            Mapper[A, KeyVal[RedK, RedV]](out.head, 8)(f.asInstanceOf[A => Traversable[KeyVal[RedK, RedV]]]) :: out
          case ReduceFunction(f) =>
            Reducer[RedK, RedV](out.head, 8)(f.asInstanceOf[(RedV, RedV) => RedV]) :: out
        }
    }
}

object pmap {
  def apply[A: ClassTag, RedK: ClassTag, RedV: ClassTag]
  (myFunc: A => Traversable[KeyVal[RedK, RedV]])
  (implicit context: ActorContext): Pipeline[A, RedK, RedV] =
    Pipeline(List(MapFunction(myFunc)))
}

object preduce {
  def apply[A: ClassTag, RedK: ClassTag, RedV: ClassTag]
  (myFunc: (RedV, RedV) => RedV)
  (implicit context: ActorContext): Pipeline[A, RedK, RedV] =
    Pipeline(List(ReduceFunction(myFunc)))
}

sealed trait MRFunction

case class MapFunction[A, RedK, RedV](mapFun: A => Traversable[KeyVal[RedK, RedV]]) extends MRFunction

case class ReduceFunction[RedK, RedV](redFun: (RedV, RedV) => RedV) extends MRFunction

