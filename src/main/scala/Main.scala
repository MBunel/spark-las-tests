import io.pdal._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {
  // Initialisation of spark session
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Testtouille")
    .getOrCreate();

  // Check the initialisation
  println(spark)
  println("Spark Version : " + spark.version)

  // Each value is a LiDAR HD block (approx. 2500 km2 and 10E9 points by bloc)
  val liste_nuages = Seq(
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/SP/classification/2_Nuage_Classe/",
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/RQ/classification/Nuage_travail",
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/QR/classification/Nuage_travail",
    "/run/user/22825/gvfs/smb-share:server=store,share=store-lidarhd/production/chantiers/QQ/classification/Nuage_travail/"
  )

  // We create a dataframe from 4 blocks
  val las_dataframe = spark.read
    .format("LAS.LAS")
    // The option "las_reader" choose the lib for read. Two
    // values allowed "las4j" and "pdal" (slow)
    // See LAS.LASOptions for more info
    .option("las_reader", "las4j")
    .load(liste_nuages: _*)

  // Check import
  las_dataframe.printSchema()
  las_dataframe.show(50)

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

  // Write a sample. Un comment for write
  val las_dataframe_sub = las_dataframe
    .sample(0.000001)
  //.write
  //.csv("/home/MBunel/Bureau/SP_sample_2.csv")

}
