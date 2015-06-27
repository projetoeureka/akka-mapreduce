package geekie.mapred

import akka.actor.{Actor, ActorRef}
import geekie.mapred.io.{FileChunkLineReader, DataChunk}

import scala.reflect.ClassTag

/**
 * Created by nlw on 08/04/15.
 * A mapper that takes an object (iterator) and forwards zero or more corresponding calculated objects to the reducer.
 *
 */
class MapperTask[A: ClassTag, B](output: ActorRef, f: A => TraversableOnce[B]) extends Actor {
  def receive = {
    case datum: A => f(datum) foreach (output ! _)
    case dataItr: Iterator[A] => dataItr flatMap f foreach (output ! _)
    case DataChunk(chunk: FileChunkLineReader, n, limit) =>
      try {
        val dataItr = if (limit.isDefined) chunk.iterator.take(limit.get) else chunk.iterator
        dataItr flatMap f.asInstanceOf[String => TraversableOnce[B]] foreach (output ! _)
        output ! ProgressReport(n)
      } finally {
        chunk.close()
      }

    case Forward(x) => output forward x
  }
}


object MapperTask {
  def apply[A: ClassTag, B](output: ActorRef)(f: A => TraversableOnce[B]) = new MapperTask(output, f)
}
