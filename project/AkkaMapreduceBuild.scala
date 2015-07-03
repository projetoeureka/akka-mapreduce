import sbt._
import Keys._

object AkkaMapreduceBuild extends Build {

  val dependencies = {
    val akkaV       = "2.3.9"
    val akkaStreamV = "1.0-M5"
    val scalaTestV  = "2.2.1"
    Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "org.scala-lang" % "scala-reflect" % "2.11.6",
      "org.json4s" %% "json4s-native" % "3.2.11"
    )
  }

  lazy val AkkaMapreduceProject = Project("akka-mapreduce", file(".")) settings(
    version       := "1.0",
    scalaVersion  := "2.11.6",
    libraryDependencies ++= dependencies
  )
}


