import sbt._
import Keys._

object Dependencies {
  val Scala213 = "2.13.12"

  val Versions = Map(
    "Scalamock" -> "5.1.0",
    "Scalatest" -> "3.2.18",
    "UpCore" -> "1.5.7",
    "UpJava" -> "0.1.10",
    //TODO update to 0.2
  )

  val Scalatest = "org.scalatest" %% "scalatest" % Versions("Scalatest") % Test
  val Scalamock = "org.scalamock" %% "scalamock" % Versions("Scalamock") % Test
  val Uprotocol = "org.eclipse.uprotocol" % "up-java" % Versions("UpJava")
}
