/**
 * Created by nlw on 06/04/15. Line-oriented file chunk reading utilities.
 *
 */
package geekie.mapred.io

import scala.io.Source

object FileChunkLineReader {
  def apply(filename: String)(range: (Int, Int)) =
    Source.fromIterable(FileChunkCharReader(filename)(range)).getLines()
}

class FileChunkCharReader(filename: String)(range: (Int, Int)) extends Iterable[Char] {
  val (start, end) = range
  def fullItr = Source.fromFile(filename).zipWithIndex

  val dumbItr = (if (start > 0) fullItr.drop(start).dropWhile(_._1 != '\n').drop(1) else fullItr).buffered

  def iterator = if (dumbItr.hasNext && dumbItr.head._2 <= end) new Iterator[Char] {
    def hasNext = dumbItr.hasNext && (dumbItr.head._2 < end || dumbItr.head._1 != '\n')

    def next() = dumbItr.next()._1
  }
  else Iterator[Char]()
}

object FileChunkCharReader {
  def apply(filename: String)(range: (Int, Int)) = new FileChunkCharReader(filename)(range)
}

object ChunkLimits {
  def apply(end: Int, skip: Int) =
      ((0 until end by skip) zip (skip until end by skip)) :+ ((end - 1) / skip * skip, end)
}

case class FileChunk(filename: String, begin: Int, end: Int)
