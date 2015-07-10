package geekie.mapred

import akka.actor.{Actor, Props, ActorRef}
import akka.routing.{SmallestMailboxPool, Broadcast}

import scala.reflect.ClassTag

/**
 * Created by nlw on 15/04/15.
 * An akka-mapreduce mapper, with a router and decimator
 */
class Mapper[A: ClassTag, B: ClassTag](output: ActorRef, nMappers: Int, f: A => TraversableOnce[B], lazyMap: Boolean = false) extends Actor {
  def myMapper = MapperTask(output, lazyMap = lazyMap)(f)

  val mapperRouter = context.actorOf(SmallestMailboxPool(nMappers).props(Props(myMapper)), "mapper-router")

  def receive = {
    case Forward(x) => output forward x
    case ForwardToReducer(x) => output forward ForwardToReducer(x)
    case ProgressReport(x) => output forward ProgressReport(x)
    case x: Any => mapperRouter forward x
  }
}

object Mapper {
  def apply[A: ClassTag, B: ClassTag](output: ActorRef, nWorkers: Int, isLazy:Boolean, index: Int)(f: A => TraversableOnce[B])
                                     (implicit context: akka.actor.ActorContext) =
    context.actorOf(Props(new Mapper[A, B](output, nWorkers, f, lazyMap = isLazy)), s"mapper-$index")
}
