package LAS

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{
  FloatType,
  ShortType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class LASTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat]
) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = Some(
    StructType(
      Array(
        StructField("X", FloatType, nullable = false),
        StructField("Y", FloatType, nullable = false),
        StructField("Z", FloatType, nullable = false),
        StructField("Classification", ShortType, nullable = false)
      )
    )
  )

  override def formatName: String = "LAS"

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): LASScanBuilder =
    new LASScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  //override def supportsDataType(dataType: DataType): Boolean = super.supportsDataType(dataType)

}
