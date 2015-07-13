import sbt._
import Keys._

object AkkaMapreduceBuild extends Build {

  val dependencies = {
    val akkaV       = "2.3.11"
    val akkaStreamV = "1.0-RC4"
    val scalaTestV  = "2.2.1"
    Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaV,
      "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
      "org.json4s" %% "json4s-native" % "3.2.11"
    )
  }

  lazy val AkkaMapreduceProject = Project("akka-mapreduce", file(".")) settings(
    version       := "1.0",
    scalaVersion  := "2.11.7",
    libraryDependencies ++= dependencies
  )
}


