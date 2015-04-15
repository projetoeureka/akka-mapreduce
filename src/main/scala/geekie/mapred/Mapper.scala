package geekie.mapred

import akka.actor.{Actor, Props, ActorRef}
import akka.routing.{SmallestMailboxPool, Broadcast}

import scala.reflect.ClassTag

/**
 * Created by nlw on 15/04/15.
 * An akka-mapreduce mapper, with a router and decimator
 */
object Mapper {
  def apply[A: ClassTag, B: ClassTag](output: ActorRef, nWorkers: Int)
                                     (f: A => Seq[B])(implicit context: akka.actor.ActorContext) =
    context.actorOf(Props(new Mapper[A, B](output, nWorkers, f)), "mapper")
}

class Mapper[A: ClassTag, B: ClassTag](output: ActorRef, nMappers: Int, f: A => Seq[B]) extends Actor {
  val mapperDecimator = context.actorOf(Props(classOf[Decimator], output, nMappers, EndOfData), "mapper-decimator")

  def myMapper = MapperTask(mapperDecimator)(f)

  val mapperRouter = context.actorOf(SmallestMailboxPool(nMappers).props(Props(myMapper)), "mapper-router")

  def receive = {
    case EndOfData => mapperRouter ! Broadcast(Forward(EndOfData))
    case x: Any => mapperRouter ! x
  }
}