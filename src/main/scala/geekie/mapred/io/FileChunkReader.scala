package geekie.mapred.io

/**
 * Created by nlw on 06/04/15. Line-oriented file chunk reading utilities.
 */

import scala.io.Source

object FileChunkLineReader {
  def apply(filename: String, start: Int, end: Int) =
    Source.fromIterable(FileChunkCharReader(filename, start, end)).getLines()
}

class FileChunkCharReader(filename: String, start: Int, end: Int) extends Iterable[Char] {
  def fullItr = Source.fromFile(filename).zipWithIndex

  val dumbItr = (if (start > 0) fullItr.drop(start).dropWhile(_._1 != '\n').drop(1) else fullItr).buffered

  def iterator = if (dumbItr.hasNext && dumbItr.head._2 <= end) new Iterator[Char] {
    def hasNext = dumbItr.hasNext && (dumbItr.head._2 < end || dumbItr.head._1 != '\n')

    def next() = dumbItr.next()._1
  }
  else Iterator[Char]()
}

object FileChunkCharReader {
  def apply(filename: String, start: Int, end: Int) = new FileChunkCharReader(filename, start, end)
}

object ChunkLimits {
  def apply(end: Int, skip: Int) = ((0 until end by skip) zip (skip until end by skip)) :+((end - 1) / skip * skip, end)
}

case class FileChunk(filename: String, begin: Int, end: Int)
