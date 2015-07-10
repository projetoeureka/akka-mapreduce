package geekie.mapred

import akka.actor.{ActorContext, ActorRef}

import scala.reflect.ClassTag

sealed trait MRBuildCommand

sealed trait MRBuildWorkerCommand extends MRBuildCommand {
  def buildWorker(output: ActorRef, nw: Int, lw:Boolean, ind: Int)(implicit context: ActorContext): ActorRef
}

case class MapperFuncCommand[A: ClassTag, B: ClassTag](mapFun: A => TraversableOnce[B]) extends MRBuildWorkerCommand {
  override def buildWorker(output: ActorRef, nw: Int, lw:Boolean, ind: Int)(implicit context: ActorContext): ActorRef =
    Mapper[A, B](output, nw, lw, ind)(mapFun)
}

case class ReducerFuncCommand[K: ClassTag, V: ClassTag](redFun: (V, V) => V) extends MRBuildWorkerCommand {
  override def buildWorker(output: ActorRef, nw: Int, lw:Boolean, ind: Int)(implicit context: ActorContext): ActorRef =
    Reducer[K, V](output, nw, ind)(redFun)
}

case class MultiplyWorkers(nw: Int) extends MRBuildCommand

case class LazyMap(isLazy: Boolean) extends MRBuildCommand
