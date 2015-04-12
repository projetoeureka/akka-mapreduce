# akka-wordcount

This is an Akka based library to perform parallel Map-Reduce tasks, with all sub-tasks happening inside Akka actors.
Includes a wordcount example, and a library to help reading large line-oriented files in blocks of whole lines.

The process happens as a pipe, with components in series and with little supervision, but there are mechanisms to
monitor the progress, and to ensure that all the data has been processed, and therefore the job is finished. This
properjob termination tends to be one of the major issues when trying to implement a Map/Reduce framework in Akka.

## `Decimator` and `EndOfData`
The `EndOfData` message must go through each of the single workers, and the `Decimator` actor is used to transform the
set of messages into a single one signaling the end of the last sub-task from that step.
