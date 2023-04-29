ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "fpl-data-calculator"
  )

val spark_version = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,

  "com.github.mrpowers" %% "spark-daria" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)
