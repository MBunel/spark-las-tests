ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "Test_pdal_scala"
  )

//javaOptions +=

libraryDependencies ++= Seq(
  "com.github.mreutegg" % "laszip4j" % "0.14",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "io.pdal" %% "pdal" % "2.3.1",        // core library
  "io.pdal" %  "pdal-native" % "2.3.1", // jni binaries
  "io.pdal" %% "pdal-scala" % "2.3.1"   // if scala core library (if required)
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "latest.integration" % Test
)
