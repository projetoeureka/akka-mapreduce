package geekie.mapred

import akka.actor.{ActorContext, ActorRef}

import scala.reflect.ClassTag

/**
 * Created by nlw on 17/04/15.
 * A pipeline builder
 */
case class Pipeline[A: ClassTag, RedK: ClassTag, RedV: ClassTag](functionSeq: List[MRBuildCommand])(implicit context: ActorContext) {

  type MapperFunction = A => Traversable[KeyVal[RedK, RedV]]
  type ReducerFunction = (RedV, RedV) => RedV

  def map(myFunc: MapperFunction): Pipeline[A, RedK, RedV] =
    Pipeline(MapperFuncCommand(myFunc) :: functionSeq)

  def reduce(myFunc: ReducerFunction): Pipeline[A, RedK, RedV] =
    Pipeline(ReducerFuncCommand(myFunc) :: functionSeq)

  def times(nWorkers: Int): Pipeline[A, RedK, RedV] =
    Pipeline(MultiplyWorkers(nWorkers) ::  functionSeq)

  def output(dest: ActorRef) = generatePipeline(dest)

  def generatePipeline(dest: ActorRef) = {
    ((List(dest), 1) /: functionSeq) {
      (state, buildCommand) => {
        val (out: List[ActorRef], nw: Int) = state
        buildCommand match {
          case MultiplyWorkers(newNw) => (out, newNw)
          case MapperFuncCommand(func) =>
            (Mapper[A, KeyVal[RedK, RedV]](out.head, nw)(func.asInstanceOf[MapperFunction]) :: out, 1)
          case ReducerFuncCommand(func) =>
            (Reducer[RedK, RedV](out.head, nw)(func.asInstanceOf[ReducerFunction]) :: out, 1)
        }
      }
    }._1
  }
}

object pipe_map {
  def apply[A: ClassTag, RedK: ClassTag, RedV: ClassTag]
  (myFunc: A => Traversable[KeyVal[RedK, RedV]])
  (implicit context: ActorContext): Pipeline[A, RedK, RedV] =
    Pipeline(List(MapperFuncCommand(myFunc)))
}

object pipe_reduce {
  def apply[A: ClassTag, RedK: ClassTag, RedV: ClassTag]
  (myFunc: (RedV, RedV) => RedV)
  (implicit context: ActorContext): Pipeline[A, RedK, RedV] =
    Pipeline(List(ReducerFuncCommand(myFunc)))
}

sealed trait MRBuildCommand

case class MapperFuncCommand[A, RedK, RedV](mapFun: A => Traversable[KeyVal[RedK, RedV]]) extends MRBuildCommand

case class ReducerFuncCommand[RedK, RedV](redFun: (RedV, RedV) => RedV) extends MRBuildCommand

case class MultiplyWorkers(nw: Int) extends MRBuildCommand
