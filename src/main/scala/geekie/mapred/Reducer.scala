package geekie.mapred

import akka.actor.{Actor, Props, ActorRef}
import akka.routing.{Broadcast, ConsistentHashingPool}

import scala.reflect.ClassTag

/**
 * Created by nlw on 15/04/15.
 * An akka-mapreduce reducer, with a router and decimator
 */
class Reducer[K: ClassTag, V: ClassTag](output: ActorRef, nReducers: Int, f: (V, V) => V) extends Actor {
  val reducerDecimator = context.actorOf(Props(classOf[Decimator], output, nReducers, EndOfData), "reducer-decimator")

  def myReducer = ReducerTask[K, V](reducerDecimator)(f)

  val reducerRouter = context.actorOf(ConsistentHashingPool(nReducers).props(Props(myReducer)), "reducer-router")

  def receive = {
    case EndOfData =>
      reducerRouter ! Broadcast(GetAggregator)
      reducerRouter ! Broadcast(Forward(EndOfData))
    case x: Any => reducerRouter forward x
  }
}

object Reducer {
  def apply[K: ClassTag, V: ClassTag](output: ActorRef, nWorkers: Int)(f: (V, V) => V)
                                     (implicit context: akka.actor.ActorContext) =
    context.actorOf(Props(new Reducer[K, V](output, nWorkers, f)), "reducer")
}

// TODO: generic worker names
