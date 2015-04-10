/**
 * Created by nlw on 06/04/15. Line-oriented file chunk reading utilities.
 *
 */
package geekie.mapred.io

import java.io._
import java.nio.charset.StandardCharsets

class FileChunkLineReader(filename: String, start: Int, end: Int) extends Iterable[String] {
  val fp = new RandomAccessFile("/home/nlw/machado.txt", "r")
  if (start > 0) fp.seek(start - 1)
  val itr = new BufferedReader(
    new InputStreamReader(
      new FileInputStream(fp.getFD)))

  if (start > 0) itr.readLine()

  var pos: Long = start
  println(f"file chunk $start $end $pos")

  def iterator = new Iterator[String] {
    def hasNext = pos < end

    def next() = {
      val xx = itr.readLine()
      pos += xx.getBytes.length + 1
      xx
    }
  }
}


object FileChunkLineReader {
  def apply(filename: String)(range: (Int, Int)) = new FileChunkLineReader(filename, range._1, range._2)
}


object ChunkLimits {
  def apply(end: Int, skip: Int) =
    ((0 until end by skip) zip (skip until end by skip)) :+((end - 1) / skip * skip, end)
}

case class FileChunk(filename: String, begin: Int, end: Int)


//try {
//  Path path = Paths.get ("/home/temp/", "hugefile.txt");
//  SeekableByteChannel sbc = Files.newByteChannel(path, StandardOpenOption.READ);
//  ByteBuffer bf = ByteBuffer.allocate (941); // line size
//  int i = 0;
//  while ((i = sbc.read (bf) ) > 0) {
//    bf.flip ();
//    System.out.println (new String (bf.array () ) );
//    bf.clear ();
//  }
//} catch (Exception e) {
//    e.printStackTrace ();
//}