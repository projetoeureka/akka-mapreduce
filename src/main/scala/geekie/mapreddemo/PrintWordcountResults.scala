package geekie.mapreddemo

object PrintWordcountResults {
  def apply[RedK, RedV](ag: Map[RedK, RedV])(implicit ev: Numeric[RedV]) = {
    import ev._
    println("FINAL RESULTS")
    //val ag = finalAggregate.asInstanceOf[Map[String, Long]]
    ag.toList sortBy (-_._2) take 20 foreach {
      case (s, i) => println(f"$s%8s:${i.toInt}%5d")
    }
  }
}
