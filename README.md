# akka-mapreduce

`akka-mapreduce` is a Scala and Akka based library to run map-reduce jobs with all "map" and "reduce" tasks running in parallel, using Akka actors in Scala. There are a couple of Akka map-reduce examples out there, but few of them take good care to signal when the job is done, with all data being properly processed. Most oversimple approaches to map-reduce don't make sure every single bit of data has been accounted for, and some also discard input records with improper splitting of the input. This kind of problem is nonexistent if you are using Hadoop Streaming, for instance. Our framework provides the tools you need to create simple map-reduce applications that are free from these basic problems.

This project should be useful for map-reduce jobs that are big enough to make parallel processing desirable, but not as big as to require a bad-ass cluster with 100s of machines. Our wiki has [a section discussing different problem scenarios](https://github.com/projetoeureka/akka-mapreduce/wiki/MapReduce-Problem-Scenarios). We believe there are some cases our framework offers a better alternative than either Hadoop Streaming or than Scala parallel collections, for instance.

This project includes the library, and also a word-count example to illustrate how to use the framework. If you are interested in trying out you should probably just start by making modifications to that code. _(Actually, right now we still have more relevant code in the example application than in the actual framework, maybe we should re-factor a bit...)_

#### The akka-mapreduce pipeline
The processing in `akka-mapreduce` happens in a pipeline, with components connected in series and with little supervision, but there are mechanisms to monitor the progress, and to ensure that all the data has been processed, and therefore the job is finished. This proper job termination tends to be one of the major issues when trying to implement a map-reduce framework in Akka.

Figure illustrates a typical application. A job in `akka-mapreduce` begins with a supervisor actor sending the input data to a router that distributes the data to actors performing the map task. The output from the mappers eventually reach another router, which distributes it to reducer actors. The results from the reducers are then eventually transmitted back to the supervisor. The multiple independent mapper and reducer actors are responsible for paralellizing the processing, and Akka takes care of all the scheduling. You just need to choose an appropriate number of workers, and maybe take care not to overflow the message boxes by pushing in too much data.

![akka-mapreduce pipeline](https://raw.githubusercontent.com/wiki/projetoeureka/akka-mapreduce/images/Akka-Map-Reduce.png)

#### Job end signaling with `Decimator` and `EndOfData`
One of the biggest challenges in actor systems is usually to synchronize actors, and it is not different in akka-mapreduce. Because the actors work in a non-centralized way, there is no easy way for the supervisor to know if the processing had ended. The input data may have been sent to the mappers, but there may still be messages floating around if the supervisor decides to pull the results from the reducers.

The way we found to figure out if all the data has been processed by the pipeline in order to declare a job "finished" was to transmit a special message, `EndOfData` in the example application. This message is broadcast to all the _Nm_ mappers, who forwards them. Therefore receiving the _Nm_-th message indicates all workers are finished, since their message boxes have all beem "cleared" by an `EndOfData` message.

In order to handle these multiple messages, turning them into a single message again to follow in the pipeline, we introduce a `Decimator` actor, whose job is just to remove _Nm_-1 `EndOfData` messages, and forward only the last one. This way the same process can happen in the reducer stage, or other stages, and when the final destination of the results receives the signal, it means the job is done.
