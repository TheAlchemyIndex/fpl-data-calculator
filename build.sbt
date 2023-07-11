ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "fpl-understat-processor"
  )

val spark_version = "3.3.2"
val spark_daria_version = "1.2.3"
val scala_test_version = "3.2.15"
val scala_logging_version = "3.9.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,

  "com.typesafe" % "config" % "1.4.2",
  "com.typesafe.scala-logging" %% "scala-logging" % scala_logging_version,

  "com.github.mrpowers" %% "spark-daria" % spark_daria_version,
  "org.scalatest" %% "scalatest" % scala_test_version % "test"
)
