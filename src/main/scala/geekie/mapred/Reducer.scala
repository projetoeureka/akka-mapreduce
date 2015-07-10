package geekie.mapred

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.routing.BalancingPool

import scala.reflect.ClassTag

/**
 * Created by nlw on 15/04/15.
 * An akka-mapreduce reducer, with a router and decimator
 */
class Reducer[K: ClassTag, V: ClassTag](output: ActorRef, nReducers: Int, f: (V, V) => V) extends Actor {
  def myReducer = ReducerTask[K, V](output)(f)

  val reducerRouter = context.actorOf(Props(myReducer).withRouter(BalancingPool(nReducers)), "reducer-router")
  context.watch(reducerRouter)

  def receive = {
    case Forward(x) => output forward x
    case ForwardToReducer(x) => self forward x
    case ProgressReport(n) =>
      reducerRouter ! Forward(ProgressReport(n))
    case EndOfData =>
      for (_ <- 1 to nReducers) reducerRouter ! GetAggregator
    case Terminated(`reducerRouter`) =>
      output ! EndOfData
    case x: Any =>
      reducerRouter forward x
  }
}

object Reducer {
  def apply[K: ClassTag, V: ClassTag](output: ActorRef, nWorkers: Int, index: Int)(f: (V, V) => V)
                                     (implicit context: akka.actor.ActorContext) =
    context.actorOf(Props(new Reducer[K, V](output, nWorkers, f)), s"reducer-$index")
}
