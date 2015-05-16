package geekie.mapred

import scala.io.Source

object StopWords {
  private val stopWords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopWords.contains(s)
}
