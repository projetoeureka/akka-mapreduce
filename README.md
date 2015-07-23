# akka-mapreduce

`akka-mapreduce` is a Scala and Akka based library to run map-reduce jobs with all "map" and "reduce" tasks running in parallel, using Akka actors in Scala. In our framework data is initially read using Akka Stream, and what we do is to provide an [ActorSubscriber](http://doc.akka.io/docs/akka-stream-and-http-experimental/1.0/scala/stream-integrations.html) that can be used as a sink to the stream, processing data chunks in parallel, and aggregating the results from the multiple reducers when the stream finishes.

Our wiki has [a section discussing different scenarios](https://github.com/projetoeureka/akka-mapreduce/wiki/MapReduce-Problem-Scenarios) of map-reduce data processing problems. We believe there are some specific cases our framework offers a better alternative than either Hadoop Streaming or than Scala parallel collections, for instance. Our project is aimed at cases where you can have just a monolithic application running in a single multi-core machine, and with the output data able to fit the available RAM memory. Processing happens completely in-memory, unlike how Hadoop Streaming works. Spark is able to run analises in-memory too, but we think using our `Mapreduce` actor along with Akka Stream could be more practical in some cases.

## How to run

This project includes the library, and also a “wordcount” example to illustrate how to use the framework. If you are interested in trying out you should probably just start by making modifications to that code. But here is a quick taste of how it works. First of all you'll need to either clone this Git project or copy the source code, because I haven't learned how to put a packe in Maven yet! But here is a simple example:


```scala
package geekie.mapreddemo

import akka.actor.Actor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import geekie.mapred._

class SimpleExample extends Actor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()

  val theData = "Antidisestablishmentarianism".toList

  val nWorkers = 4
  val chunkSize = theData.size / nWorkers

  def mapFun(ch: Char) = Some(MapredWorker.KeyVal(ch.toLower, 1))

  val mapredProps = Mapred.props(nWorkers)(mapFun)(_ + _) { counters =>
    println(s"FINAL RESULTS\n$counters")
    context stop self
  }

  Source(theData).grouped(chunkSize).to(Sink.actorSubscriber(mapredProps)).run()

  override def receive = {
    case _ =>
  }
}
``` 

This code must be run like this

```
sbt 'runMain akka.Main geekie.mapreddemo.SimpleExample'
```

The expected output is
```
[info] Running akka.Main geekie.mapreddemo.SimpleExample
[INFO] [07/23/2015 00:51:05.303] (...) INPUT CONSUMED - FINISHING PROCESSING
[INFO] [07/23/2015 00:51:05.305] (...) DONE: 1 chunks
[INFO] [07/23/2015 00:51:05.305] (...) DONE: 2 chunks
[INFO] [07/23/2015 00:51:05.305] (...) DONE: 3 chunks
[INFO] [07/23/2015 00:51:05.310] (...) DONE: 4 chunks
[INFO] [07/23/2015 00:51:05.551] (...) REDUCER AGGREGATION COMPLETED
FINAL RESULTS
Map(e -> 2, s -> 4, n -> 3, t -> 3, a -> 4, m -> 2, i -> 5, b -> 1, l -> 1, h -> 1, r -> 1, d -> 1)
[INFO] [07/23/2015 00:51:05.558] (...) application supervisor has terminated, shutting down
[success] Total time: 30 s, completed 23/07/2015 00:51:05

```

The method `Mapred.props` receives as arguments the desired number of parallel workers, the mapper function to be applied to the input data, the reducer function to be applied to values corresponding to each key, and a final function that receives the aggregated data after the stream is completed.
