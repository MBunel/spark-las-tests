package LAS

import com.github.mreutegg.laszip4j.LASReader
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

import java.io.File

/** Common functions to treat las data file
  */
abstract class LASDataSource extends Serializable {
  def inferSchema(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: LASOptions
  ): Option[StructType] = {
    if (inputPaths.nonEmpty) {
      Some(infer(sparkSession, inputPaths, parsedOptions))
    } else {
      None
    }
  }

  protected def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: LASOptions
  ): StructType

  def fieldExtractor(structField: StructField): StructField => DataType

  def readFile(): Iterator[InternalRow] = ???

}

object LASDataSource extends Logging {
  def apply(options: LASOptions): LASDataSource = {
    options.las_reader match {
      case "las4j" => Las4JDataSource
      case "pdal"  => PdalDataSource
    }
  }
}

object Las4JDataSource extends LASDataSource {
  override def fieldExtractor(
      structField: StructField
  ): StructField => DataType = ???

  def createBaseDataset(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: LASOptions
  ): Dataset[String] = ???
  override protected def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: LASOptions
  ): StructType = {
    // Write something for infer a file schema

    //val headers: Dataset[String] = createBaseDataset(sparkSession, inputPaths, parsedOptions)

    // This fun made a seq of las headers
    val test = inputPaths
      .map(_.getPath)
      .map(getPathWithoutSchemeAndAuthority(_).toString)
    val tt = test.map(x => new LASReader(new File(x)).getHeader)

    // Return a hardcoded schema
    val schema = tt
      .map(x => x.getPointDataRecordFormat)
      .map(x => StructType(LASDimension.pointFormatToSchema(x)))
    // Just for test
    val yo = LASDimension.pointFormatToSchema(dataRecordFormat = 6);
    val yA = LASDimension.pointFormatToSchema(dataRecordFormat = 2);

    StructType(yo)
  }
}

object PdalDataSource extends LASDataSource {
  override def fieldExtractor(
      structField: StructField
  ): StructField => DataType = ???

  override protected def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: LASOptions
  ): StructType = ???
}
