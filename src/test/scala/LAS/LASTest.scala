package LAS

import org.apache.spark.sql.SparkSession

class LASTest extends org.scalatest.funsuite.AnyFunSuiteLike {
  private val KMlas = "test-data/KM_1.las"
  private val KMlaz = "test-data/KM_1.laz"
  // Multiple versions
  private val KMlas_11 = "test-data/1.1/KM_1.las"
  private val KMlaz_11 = "test-data/1.1/KM_1.laz"
  private val KMlas_12 = "test-data/1.2/KM_1.las"
  private val KMlaz_12 = "test-data/1.2/KM_1.laz"
  private val KMlas_13 = "test-data/1.3/KM_1.las"
  private val KMlaz_13 = "test-data/1.3/KM_1.laz"
  private val KMlas_14 = "test-data/1.4/KM_1.las"
  private val KMlaz_14 = "test-data/1.4/KM_1.laz"

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

  test("All las version reading") {
    val KM_11 = spark.read.format("LAS.LAS").load(getTestFile(KMlas_11))
    val KM_12 = spark.read.format("LAS.LAS").load(getTestFile(KMlas_12))
    val KM_13 = spark.read.format("LAS.LAS").load(getTestFile(KMlas_13))
    val KM_14 = spark.read.format("LAS.LAS").load(getTestFile(KMlas_14))

    assert(KM_11.count() == 5742)
    assert(KM_12.count() == 5742)
    assert(KM_13.count() == 5742)
    assert(KM_14.count() == 5742)
  }
  test("All las version (compressed) reading") {
    val KM_11 = spark.read.format("LAS.LAS").load(getTestFile(KMlaz_11))
    val KM_12 = spark.read.format("LAS.LAS").load(getTestFile(KMlaz_12))
    val KM_13 = spark.read.format("LAS.LAS").load(getTestFile(KMlaz_13))
    val KM_14 = spark.read.format("LAS.LAS").load(getTestFile(KMlaz_14))

    assert(KM_11.count() == 5742)
    assert(KM_12.count() == 5742)
    assert(KM_13.count() == 5742)
    assert(KM_14.count() == 5742)
  }

  test("Multi version reading") {
    val files = Seq(getTestFile(KMlaz_11), getTestFile(KMlas_14))
    val KM = spark.read.format("LAS.LAS").load(files: _*)
    assert(KM.count() == 11484)
  }

  // Write test

}
