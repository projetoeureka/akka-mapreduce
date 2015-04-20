package geekie.mapred

import akka.actor.{ActorContext, ActorRef}

import scala.reflect.ClassTag

/**
 * Created by nlw on 17/04/15.
 * A pipeline builder
 */
case class Pipeline[A:ClassTag](functionSeq: List[MRBuildCommand])(implicit context: ActorContext) {

  def mapkv[K:ClassTag, V:ClassTag](myFunc: A => Traversable[KeyVal[K,V]]) =
    PipelineKV[K, V](MapperFuncCommand[A, KeyVal[K,V]](myFunc) :: functionSeq)

  def map[B:ClassTag](myFunc: A => Traversable[B]) =
    Pipeline[B](MapperFuncCommand[A, B](myFunc) :: functionSeq)

  def times(nWorkers: Int) =
    Pipeline[A](MultiplyWorkers(nWorkers) :: functionSeq)

  def output(dest: ActorRef) = generatePipeline(dest)

  def generatePipeline(dest: ActorRef) = {
    ((List(dest), 1) /: functionSeq) {
      (state, buildCommand) => {
        val (out: List[ActorRef], nw: Int) = state
        buildCommand match {
          case MultiplyWorkers(newNw) => (out, newNw)
          case com: MRBuildWorkerCommand => (com.buildWorker(out.head, nw) :: out, 1)
        }
      }
    }._1
  }
}

case class PipelineKV[K:ClassTag, V:ClassTag](functionSeq: List[MRBuildCommand])(implicit context: ActorContext) {
  def times(nWorkers: Int) =
    PipelineKV[K,V](MultiplyWorkers(nWorkers) :: functionSeq)

  def reduce(myFunc: (V, V) => V) =
    Pipeline[V](ReducerFuncCommand[K, V](myFunc) :: functionSeq)
}

object pipe_map {
  def apply[A: ClassTag, B: ClassTag]
  (myFunc: A => Traversable[B])
  (implicit context: ActorContext) =
    Pipeline[B](List(MapperFuncCommand[A, B](myFunc)))
}

object pipe_mapkv {
  def apply[A: ClassTag, K:ClassTag, V:ClassTag]
  (myFunc: A => Traversable[KeyVal[K,V]])
  (implicit context: ActorContext) =
    PipelineKV[K,V](List(MapperFuncCommand[A, KeyVal[K,V]](myFunc)))
}


sealed trait MRBuildCommand

sealed trait MRBuildWorkerCommand extends MRBuildCommand {
  def buildWorker(output: ActorRef, nw: Int)(implicit context: ActorContext): ActorRef
}

case class MapperFuncCommand[A: ClassTag, B: ClassTag](mapFun: A => Traversable[B]) extends MRBuildWorkerCommand {
  override def buildWorker(output: ActorRef, nw: Int)(implicit context: ActorContext): ActorRef = Mapper[A, B](output, nw)(mapFun)
}

case class ReducerFuncCommand[K:ClassTag, V: ClassTag](redFun: (V, V) => V) extends MRBuildWorkerCommand {
  override def buildWorker(output: ActorRef, nw: Int)(implicit context: ActorContext): ActorRef =
    Reducer[K, V](output, nw)(redFun)
}

/*
case class ReducerFuncCommand[KV: TypeTag, V: ClassTag:TypeTag](redFun: (V, V) => V) extends MRBuildWorkerCommand {
  override def buildWorker(output: ActorRef, nw: Int)(implicit context: ActorContext): ActorRef =
    typeOf[KV] match {
      case t if t =:= typeOf[KeyVal[String, V]] =>
        Reducer[String, V](output, nw)(redFun)
    }
}
*/

case class MultiplyWorkers(nw: Int) extends MRBuildCommand
