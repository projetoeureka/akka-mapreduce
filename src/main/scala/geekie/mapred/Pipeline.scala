package geekie.mapred

import akka.actor.{ActorContext, ActorRef}

import scala.reflect.ClassTag

/**
 * Created by nlw on 17/04/15.
 * A map-reduce pipeline builder. Still gotta figure out how to avoid the "mapkv" methods, and infer the reducibility
 */
case class Pipeline[A: ClassTag, V: ClassTag](functionSeq: List[MRBuildCommand] = List())(implicit context: ActorContext) {

  def map[B: ClassTag, X](ff: A => TraversableOnce[B])(implicit fa: FunctionAdapter[A, B, X]) =
    fa.build(ff, functionSeq)

  def reduce[K: ClassTag](myFunc: (V, V) => V)(implicit ev: A <:< Reducible[K, V]) =
    Pipeline[A, V](ReducerFuncCommand[K, V](myFunc) :: functionSeq)

  def times(nWorkers: Int) =
    Pipeline[A, V](MultiplyWorkers(nWorkers) :: functionSeq)

  def lazymap(isLazy: Boolean) =
    Pipeline[A, V](LazyMap(isLazy) :: functionSeq)

  def output(dest: ActorRef) = generatePipeline(dest).actors

  def generatePipeline(dest: ActorRef) = (MRBuildState(List(dest), 1, false, 1) /: functionSeq) {
    (state, buildCommand) => state match {
      case MRBuildState(out, nw, lm, step) => buildCommand match {
        case MultiplyWorkers(newNw) => MRBuildState(out, newNw, lm, step)
        case LazyMap(isLazy) => MRBuildState(out, nw, isLazy, step)
        case com: MRBuildWorkerCommand => MRBuildState(com.buildWorker(out.head, nw, lm, step) :: out, 1, false, step + 1)
      }
    }
  }
}

case class MRBuildState(actors: List[ActorRef], nWorkers: Int, lazyMap: Boolean, stepN: Int)

object PipelineStart {
  def apply[A: ClassTag](implicit context: ActorContext) = Pipeline[A, A](List())
}
