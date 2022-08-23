ThisBuild / version := "0.1.0-SNAPSHOT"

//ThisBuild / scalaVersion := "2.13.8"

ThisBuild / scalaVersion := "2.13"

lazy val root = (project in file("."))
  .settings(
    name := "ClearScore",
      libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1",
      libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",
      libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.13",
      libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.13" % "test"
  )
