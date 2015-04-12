package geekie.mapred.io

/**
 * Created by nlw on 06/04/15.
 * Line-oriented file chunk reading utilities. Useful for map-reduce tasks on large files.
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

object FileChunkLineReader {
  def apply(filename: String, start: Long, end: Long) = new FileChunkLineReader(filename, start, end)
  def apply(filename: String)(range: (Long, Long)) = new FileChunkLineReader(filename, range._1, range._2)
}

object ChunkLimits {
  def apply(end: Long, nChunks: Long) = chunkStream(0L, end.toLong, nChunks.toLong)

  def chunkStream(start: Long, end: Long, nChunks: Long): Stream[(Long, Long)] = {
    if (nChunks > 1) {
      val cut = start + (end - start) / nChunks
      (start, cut) #:: chunkStream(cut, end, nChunks-1)
    } else Stream[(Long, Long)]((start, end))
  }
}
