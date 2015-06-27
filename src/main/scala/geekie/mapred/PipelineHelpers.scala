package geekie.mapred

import akka.actor.ActorContext

import scala.reflect.ClassTag

object PipelineHelpers {
  def unexpected: Nothing = sys.error("Unexpected invocation")

  trait <:!<[A, B]

  implicit def nsub[A, B]: A <:!< B = new <:!<[A, B] {}

  implicit def nsubAmbig1[A, B >: A]: A <:!< B = unexpected

  implicit def nsubAmbig2[A, B >: A]: A <:!< B = unexpected

  implicit def adapt_map[A: ClassTag, B: ClassTag](implicit ev: B <:!< KeyVal[_, _]): FunctionAdapter[A, B, B] =
    new FunctionAdapter[A, B, B] {
      override def build
      (cc: A => TraversableOnce[B], cs: List[MRBuildCommand])
      (implicit context: ActorContext) =
        Pipeline[B, B](MapperFuncCommand[A, B](cc) :: cs)
    }

  implicit def adapt_mapkv[A: ClassTag, K, V: ClassTag]: FunctionAdapter[A, KeyVal[K, V], V] =
    new FunctionAdapter[A, KeyVal[K, V], V] {
      override def build
      (cc: A => TraversableOnce[KeyVal[K, V]], cs: List[MRBuildCommand])
      (implicit context: ActorContext) =
        Pipeline[KeyVal[K, V], V](MapperFuncCommand[A, KeyVal[K, V]](cc) :: cs)
    }
}

trait FunctionAdapter[A, B, V] {
  def build(ff: A => TraversableOnce[B], fs: List[MRBuildCommand])(implicit context: ActorContext): Pipeline[B, V]
}
