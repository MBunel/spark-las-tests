import IO.LAS.LASDataFrameReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {
  // Initialisation of spark session
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Testtouille")
    .config(
      "spark.executor.extraLibraryPath",
      "/home/MBunel/Documents/Code/Test_pdal_scala/libs/PDAL-2.5.6-src/build/lib"
    )
    .getOrCreate();

  // Check the initialisation
  println(spark)
  println("Spark Version : " + spark.version)

  //val base =
  //  spark.read.format("org.apache.spark.sql.execution.datasources.hbase").load()

  // Schema fusion example
  /*  val struct =
    StructType(
      StructField("a", IntegerType, true) ::
        StructField("b", LongType, false) ::
        StructField("c", BooleanType, false) :: Nil
    )

  val struct_2 =
    StructType(
      StructField("a", IntegerType, true) ::
        StructField("b", LongType, false) ::
        StructField("c", IntegerType, false) ::
        StructField("d", IntegerType, false) :: Nil
    )

  val schema_rdd = spark.sparkContext.parallelize(Seq(struct, struct_2))

  val test_union = schema_rdd.reduce((a, b) => StructType(a.concat(b).distinct))
  val test_intersects = schema_rdd.reduce((a, b) => StructType(a.intersect(b)))*/

  // READ Example
  // Each value is a LiDAR HD block (approx. 2500 km2 and 10E9 points by bloc)
  val liste_nuages = Seq(
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/SP/classification/2_Nuage_Classe/",
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/RQ/classification/Nuage_travail",
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/QR/classification/Nuage_travail",
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/QQ/classification/Nuage_travail/"
  )

  // We create a dataframe from 4 blocks
  val las_dataframe = spark.read
    // The option "las_reader" choose the lib for read. Two
    // values allowed "las4j" and "pdal" (slow)
    // See LAS.LASOptions for more info
    //.format("IO.LAS.LAS")
    .option("las_reader", "pdal")
    .las("/home/MBunel/Bureau/Barre des ecrins/")
  //.load(liste_nuages.take(1): _*)

  // Check import
  las_dataframe.printSchema()
  las_dataframe.show(50)

  val count_class = las_dataframe.agg(countDistinct("Classification"))

  // Uncomment for count the lines, very long, all points are read
  // TODO: Find a way to use the "point_count" value from las's files headers
  //println(las_dataframe.count())

  // Test SQL query
  las_dataframe.createTempView("table")
  val sql_test = spark.sql("""SELECT min(x), max(x) FROM table union all
      |SELECT min(y), max(y) FROM table union all
      |SELECT min(z), max(z) FROM table
      |""".stripMargin)

  // Uncomment to run SQL query
  //sql_test.show()

  // WRITE EXAMPLE

  // Write a sample
  //val las_dataframe_sub =
  //las_dataframe.write //.csv("/home/MBunel/Bureau/out.csv")
  //.format("LAS.LAS")
  //.save("/home/MBunel/Bureau/")

}
