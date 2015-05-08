package geekie.mapred

import akka.actor.{ActorContext, ActorRef}

import scala.reflect.ClassTag

/**
 * Created by nlw on 17/04/15.
 * A map-reduce pipeline builder. Still gotta figure out how to avoid the "mapkv" methods, and infer the reducibility
 */
case class Pipeline[A: ClassTag, V: ClassTag](functionSeq: List[MRBuildCommand])(implicit context: ActorContext) {

  //  def map[B: ClassTag](myFunc: A => Traversable[B]) = {
  //    Pipeline[B, B](MapperFuncCommand[A, B](myFunc) :: functionSeq)
  //  }

  def map[B: ClassTag, X](ff: A => Traversable[B])(implicit fa: FunctionAdapter[A, B, X]) =
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

trait FunctionAdapter[A, B, V] {
  def build(ff: A => Traversable[B], fs: List[MRBuildCommand])(implicit context: ActorContext): Pipeline[B, V]
}

object PipelineHelpers {
  def unexpected: Nothing = sys.error("Unexpected invocation")

  trait <:!<[A, B]

  implicit def nsub[A, B]: A <:!< B = new <:!<[A, B] {}

  implicit def nsubAmbig1[A, B >: A]: A <:!< B = unexpected

  implicit def nsubAmbig2[A, B >: A]: A <:!< B = unexpected

  implicit def adapt_map[A: ClassTag, B: ClassTag](implicit ev: B <:!< KeyVal[_, _]): FunctionAdapter[A, B, B] =
    new FunctionAdapter[A, B, B] {
      override def build
      (cc: A => Traversable[B], cs: List[MRBuildCommand])
      (implicit context: ActorContext) =
        Pipeline[B, B](MapperFuncCommand[A, B](cc) :: cs)
    }

  implicit def adapt_mapkv[A: ClassTag, K, V: ClassTag]: FunctionAdapter[A, KeyVal[K, V], V] =
    new FunctionAdapter[A, KeyVal[K, V], V] {
      override def build
      (cc: A => Traversable[KeyVal[K, V]], cs: List[MRBuildCommand])
      (implicit context: ActorContext) =
        Pipeline[KeyVal[K, V], V](MapperFuncCommand[A, KeyVal[K, V]](cc) :: cs)
    }
}

/*
object pipe_mapkv {
  def apply[A: ClassTag, K: ClassTag, V: ClassTag]
  =
    PipelineKV[K, V](List(MapperFuncCommand[A, KeyVal[K, V]](myFunc)))
}

object pipe_map {
  def apply[A: ClassTag, B: ClassTag]
  (myFunc:)
  (implicit context: ActorContext) =
    PipelineKV[K, V](MapperFuncCommand[A, KeyVal[K, V]](myFunc) :: functionSeq)
}
*/
