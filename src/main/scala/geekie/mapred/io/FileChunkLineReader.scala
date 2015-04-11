/**
 * Created by nlw on 06/04/15. Line-oriented file chunk reading utilities.
 *
 */
package geekie.mapred.io

/*
import java.io._
import java.nio.charset.StandardCharsets
*/



class FileChunkLineReader(filename: String, start: Int, end: Int) extends Iterable[String] {
  println(f"file chunk $start $end")

  def iterator = new Iterator[String] {
    val lineItr = new FileCounterIterator(filename)

    if (start > 0) {
      lineItr.seek(start)
      lineItr.readLine()
    }

    def hasNext = lineItr.position < end && lineItr.hasNext

    def next() = lineItr.readLine()
  }
}

object FileChunkLineReader {
  def apply(filename: String)(range: (Int, Int)) = new FileChunkLineReader(filename, range._1, range._2)
}

/*
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
*/

object ChunkLimits {
  def apply(end: Int, nChunks: Int) = {
    val skip = (end + nChunks) / nChunks
    ((0 until end by skip) zip (skip until end by skip)) :+((end - 1) / skip * skip, end)
  }
}
