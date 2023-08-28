package LAS

import org.apache.spark.sql.SparkSession

class LASTest extends org.scalatest.funsuite.AnyFunSuiteLike {
  private val KMlas = "test-data/KM_1.las"
  private val KMlaz = "test-data/KM_1.laz"

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test")
      .getOrCreate()
  }

  private def getTestFile(path: String) =
    Thread.currentThread().getContextClassLoader.getResource(path).toString

  private def verifyData() = ???

  // Read test
  test("Simple las read") {
    val KM = spark.read.format("LAS.LAS").load(getTestFile(KMlas))
    assert(KM.count() == 5742)
  }

  test("Simple laz read") {
    val KM = spark.read.format("LAS.LAS").load(getTestFile(KMlaz))
    assert(KM.count() == 5742)
  }

  test("Folder simple read") {
    val KM = spark.read.format("LAS.LAS").load(getTestFile("test-data"))
    assert(KM.count() == 11484)
  }

  test("Multi file simple read") {
    val files = Seq(getTestFile(KMlaz), getTestFile(KMlas))
    val KM = spark.read.format("LAS.LAS").load(files: _*)
    assert(KM.count() == 11484)
  }

  test("Multi folder simple read") {
    val folders = Seq(getTestFile("test-data"), getTestFile("test-data"))
    val KM = spark.read.format("LAS.LAS").load(folders: _*)
    assert(KM.count() == 22968)
  }

  // Write test

}
