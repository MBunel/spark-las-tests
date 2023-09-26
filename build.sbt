ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "Test_pdal_scala"
  )

// For use local version of laszip4j
resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
// pdal is published to maven central, but you can use the following repos in addition
resolvers ++=
  Resolver.sonatypeOssRepos("releases") ++
  Resolver.sonatypeOssRepos("snapshots")


// Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0"
)

// LasZIP4J Backend
libraryDependencies += "com.github.mreutegg" % "laszip4j" % "0.15-SNAPSHOT"
libraryDependencies++= Seq(
  "io.pdal" %% "pdal" % "2.5.1", // core library
  "io.pdal" % "pdal-native" % "2.5.1", // jni binaries
  "io.pdal" %% "pdal-scala" % "2.5.1" // if scala core library (if required)
)
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "latest.integration" % Test
)
