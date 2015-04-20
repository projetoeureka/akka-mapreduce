import sbt._
import Keys._

object AkkaWordcountBuild extends Build {

  val dependencies = {
    val akkaV       = "2.3.9"
    val akkaStreamV = "1.0-M5"
    val scalaTestV  = "2.2.1"
    Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "org.scala-lang" % "scala-reflect" % "2.11.6"
    )
  }

  lazy val AkkaWordcountProject = Project("akka-mapreduce", file(".")) settings(
    version       := "1.0",
    scalaVersion  := "2.11.6",
    // scalacOptions := Seq("-deprecation"),
    libraryDependencies ++= dependencies
  )
}


