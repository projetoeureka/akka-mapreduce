package geekie.mapred.io

/**
 * Created by nlw on 06/04/15.
 * Line-oriented file chunk reading utilities. Useful for map-reduce tasks on large files.
 *
 */

class FileChunkLineReader(filename: String, start: Int, end: Int) extends Iterable[String] {
  def iterator = new Iterator[String] {
    private val lineItr = new FileCounterIterator(filename)

    if (start > 0) lineItr.seek(start).readLine()

    def hasNext = lineItr.position <= end && lineItr.hasNext

    def next() = lineItr.readLine()
  }
}

object FileChunkLineReader {
  def apply(filename: String, start: Int, end: Int) = new FileChunkLineReader(filename, start, end)
  def apply(filename: String)(range: (Int, Int)) = new FileChunkLineReader(filename, range._1, range._2)
}

object ChunkLimits {
  def apply(end: Int, nChunks: Int) = {
    val skip = (end + 1) / nChunks
    ((0 until end by skip) zip (skip until end by skip)) :+((end - 1) / skip * skip, end)
  }
}

object BresenhamChunkLimits { //Bresenham!
  def apply(end: Int, nChunks: Int) = {
    val skip = (end + 1) / nChunks
    ((0 until end by skip) zip (skip until end by skip)) :+((end - 1) / skip * skip, end)
  }
}
