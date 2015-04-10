/**
 * Created by nlw on 06/04/15. Line-oriented file chunk reading utilities.
 *
 */
package geekie.mapred.io

import java.io._
import java.nio.charset.StandardCharsets

class FileChunkLineReader(filename: String, start: Int, end: Int) extends Iterable[String] {
  println(f"file chunk $start $end")

  def iterator = new Iterator[String] {
    val lineItr = new SmartFileIterator(filename)

    if (start > 0) {
      lineItr.seek(start)
      lineItr.readLine()
    }

    def hasNext = lineItr.position < end && lineItr.hasNext

    def next() = lineItr.readLine()
  }
}


class SmartFileIterator(filename: String) {
  val fp = new RandomAccessFile(filename, "r")
  val itr = new BufferedReader(new InputStreamReader(new FileInputStream(fp.getFD)))
  val fileSize = fp.length
  private var _position: Long = 0

  def position = _position

  def hasNext = position < fileSize

  def seek(newPosition: Long) = {
    fp.seek(newPosition)
    _position = newPosition
  }

  def readLine() = {
    val nextLine = itr.readLine()
    _position += nextLine.getBytes.length + 1
    nextLine
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