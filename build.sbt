// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.18"

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"

lazy val sparkcore = "org.apache.spark" %% "spark-core" % "3.3.0"
lazy val sparksql =  "org.apache.spark" %% "spark-sql" % "3.3.0"
lazy val hadoopclient = "org.apache.hadoop" % "hadoop-client" % "3.3.2" 

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += sparkcore
libraryDependencies += sparksql
libraryDependencies +=  hadoopclient

fork := true
