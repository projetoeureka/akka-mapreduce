package geekie.mapred.io

import java.io.{File, PrintWriter}

import akka.actor.Actor
import geekie.mapred.EndOfData
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

/**
  * An actor that receives lines to be written to a file.
  *
  */
class OutputWriter(output_file: String) extends Actor {
   val writer = new PrintWriter(new File(output_file))

   def receive = {
     case (k: JValue, v: JValue) =>
       writer.write(compact(render(k)) +"\t"+ compact(render(v)) + "\n")
     case (k: String, v: String) =>
       writer.write(f"$k\t$v\n")
     case s: String =>
       writer.write(f"$s\n")
     case EndOfData =>
       writer.close()
       context.parent ! OutputWriterFileClosed
   }
 }
