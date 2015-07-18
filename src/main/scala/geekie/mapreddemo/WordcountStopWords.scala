package geekie.mapreddemo

import scala.io.Source

object WordcountStopWords {
  private val stopWords = Source.fromFile("src/main/resources/pt_stopwords.txt").getLines().map(_.trim).toSet

  def contains(s: String) = stopWords.contains(s)
}
