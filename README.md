# akka-mapreduce

This is a Scala and Akka based library to run map-reduce jobs with all "map" and "reduce" tasks running in parallel,
using Akka actors in Scala. There are a couple of Akka map-reduce examples out there, but few of them take good care to
signal when the job is done, with all data being properly processed. Most oversimple approaches to map-reduce don't make
sure every single bit of data has been accounted for, and some also discard input records with improper splitting of the
input. This kind of problem is nonexistent if you are using Hadoop streaming, for instance.      

The current project should be useful for map-reduce jobs that are big enough to make parallel processing desirable, but
not as big as to require a bad-ass cluster with 100s of machines. Our wiki has a section discussing different problem
scenarios. We believe our framework might be a better fit than Hadoop Streaming for some of them.
 
The process happens as a pipe, with components in series and with little supervision, but there are mechanisms to
monitor the progress, and to ensure that all the data has been processed, and therefore the job is finished. This
properjob termination tends to be one of the major issues when trying to implement a Map/Reduce framework in Akka.

This project includes a wordcount example to illustrate how to use the framework. If you are interested in trying out
you should probably just start by making modifications to that code.

## `Decimator` and `EndOfData`
The `EndOfData` message must go through each of the single workers, and the `Decimator` actor is used to transform the
set of messages into a single one signaling the end of the last sub-task from that step.



