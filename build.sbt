ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "UnifillUpdateSparkPipeline"
  )
