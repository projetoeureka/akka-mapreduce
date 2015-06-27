package geekie.mapred

import akka.actor.{ActorContext, ActorRef}

import scala.reflect.ClassTag

/**
 * Created by nlw on 17/04/15.
 * A map-reduce pipeline builder. Still gotta figure out how to avoid the "mapkv" methods, and infer the reducibility
 */
case class Pipeline[A: ClassTag, V: ClassTag](functionSeq: List[MRBuildCommand]=List())(implicit context: ActorContext) {

  //  def map[B: ClassTag](myFunc: A => Traversable[B]) = {
  //    Pipeline[B, B](MapperFuncCommand[A, B](myFunc) :: functionSeq)
  //  }

  def map[B: ClassTag, X](ff: A => TraversableOnce[B])(implicit fa: FunctionAdapter[A, B, X]) =
    fa.build(ff, functionSeq)

  def reduce[K: ClassTag](myFunc: (V, V) => V)(implicit ev: KeyVal[K, V] =:= A) =
    Pipeline[A, V](ReducerFuncCommand[K, V](myFunc) :: functionSeq)

  def times(nWorkers: Int) =
    Pipeline[A, V](MultiplyWorkers(nWorkers) :: functionSeq)

  def output(dest: ActorRef) = generatePipeline(dest)

  def generatePipeline(dest: ActorRef) = ((List(dest), 1, 1) /: functionSeq) {
    (state, buildCommand) => state match {
      case (out, nw, step) => buildCommand match {
        case MultiplyWorkers(newNw) => (out, newNw, step)
        case com: MRBuildWorkerCommand => (com.buildWorker(out.head, nw, step) :: out, 1, step + 1)
      }
    }
  }._1
}

object PipelineStart {
  def apply[A: ClassTag](implicit context: ActorContext) = Pipeline[A, A](List())
}
