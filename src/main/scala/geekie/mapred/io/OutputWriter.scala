package geekie.mapred.io

import java.io.{File, PrintWriter}

import akka.actor.Actor
import geekie.mapred.EndOfData
import geekie.mapred.io.OutputWriter.OutputWriterFileClosed
import org.json4s.JsonAST.JValue
import org.json4s.native.JsonMethods._

/**
  * An actor that receives lines to be written to a file.
  *
  */

object OutputWriter {
  case object OutputWriterFileClosed
}

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
