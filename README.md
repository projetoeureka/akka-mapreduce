# akka-mapreduce

`akka-mapreduce` is a Scala and Akka based library to run map-reduce jobs with all "map" and "reduce" tasks running in parallel, using Akka actors in Scala. There are a couple of Akka map-reduce examples out there, but few of them take good care to signal when the job is done, with all data being properly processed. Most oversimple approaches to map-reduce don't make sure every single bit of data has been accounted for, and some also discard input records with improper splitting of the input. This kind of problem is nonexistent if you are using Hadoop Streaming, for instance. Our framework provides the tools you need to create simple map-reduce applications that are free from these basic problems.

This project should be useful for map-reduce jobs that are big enough to make parallel processing desirable, but not as big as to require a bad-ass cluster with 100s of machines. Our wiki has [a section discussing different problem scenarios](https://github.com/projetoeureka/akka-mapreduce/wiki/MapReduce-Problem-Scenarios). We believe there are some cases our framework offers a better alternative than either Hadoop Streaming or than Scala parallel collections, for instance.

This project includes the library, and also a word-count example to illustrate how to use the framework. If you are interested in trying out you should probably just start by making modifications to that code.

#### The akka-mapreduce pipeline
The processing in `akka-mapreduce` happens in a pipeline, with components connected in series and with little supervision, but there are mechanisms to monitor the progress, and to ensure that all the data has been processed, and therefore the job is finished. This proper job termination tends to be one of the major issues when trying to implement a map-reduce framework in Akka.

Figure illustrates a typical application. A job in `akka-mapreduce` begins with a supervisor actor sending the input data to a router that distributes the data to actors performing the map task. The output from the mappers eventually reach another router, which distributes it to reducer actors. The results from the reducers are then eventually transmitted back to the supervisor. The multiple independent mapper and reducer actors are responsible for paralellizing the processing, and Akka takes care of all the scheduling. You just need to choose an appropriate number of workers, and maybe take care not to overflow the message boxes by pushing in too much data.

Our framework allows you to control wether you want each stage to output either separate data objects to the next stage, or iterables. Keeping the data together to be processed in batches tends to improve the processing speed a lot.

![akka-mapreduce pipeline](https://raw.githubusercontent.com/wiki/projetoeureka/akka-mapreduce/images/Akka-Map-Reduce.png)

#### Job end signaling
One of the biggest challenges in actor systems is usually to synchronize actors, and it is not different in akka-mapreduce. Because the actors work in a non-centralized way, there is no easy way for the supervisor to know if the processing had ended. The input data may have been sent to the mappers, but there may still be messages floating around if the supervisor decides to pull the results from the reducers.

We are still experimenting with ways to handle the signaling of when a task is done. First we had the `Decimator` actor, but it was retired, and the previous figure is outdated. Now have the mappers reporting when they finish a chunk, and the reducers, who are now living in a `BalancingPool` router, are being terminated when there is no more data. Active development is going on in this part of the framework, but it's usable.
