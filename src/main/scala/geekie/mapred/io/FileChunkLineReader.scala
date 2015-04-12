package geekie.mapred.io

import java.io.File

/**
 * Created by nlw on 06/04/15.
 * Line-oriented file chunk reading utilities. Useful for map-reduce tasks on large files. Word of caution: because of
 * how the line-finding mechanism works, chunks should have a size of at least 2 bytes, otherwise data is just ignored.
 *
 */

class FileChunkLineReader(filename: String, start: Long, end: Long) extends Iterable[String] {
  def iterator = new Iterator[String] {
    private val lineItr = new FileCounterIterator(filename)

    if (start > 0) lineItr.seek(start).readLine()

    def hasNext = lineItr.position <= end && lineItr.hasNext

    def next() = lineItr.readLine()
  }
}

object FileChunks {
  def apply(filename: String, nChunks: Int) = ChunkLimits(FileSize(filename), nChunks) map FileChunkLineReader(filename)
}

object FileChunkLineReader {
  def apply(filename: String, start: Long, end: Long) = new FileChunkLineReader(filename, start, end)
  def apply(filename: String)(range: (Long, Long)) = new FileChunkLineReader(filename, range._1, range._2)
}

object ChunkLimits {
  def apply(end: Long, nChunks: Int) = chunkStream(0L, end, nChunks)

  def chunkStream(start: Long, end: Long, nChunks: Int): Stream[(Long, Long)] = {
    if (nChunks > 1) {
      val cut = start + (end - start) / nChunks
      (start, cut) #:: chunkStream(cut, end, nChunks - 1)
    } else (start, end) #:: Stream()
  }
}

object FileSize {
  def apply(filename: String) = new File(filename).length.toInt
}