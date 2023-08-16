package LAS

import org.apache.hadoop.fs.Path.getPathWithoutSchemeAndAuthority
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.types.StructType

/** A Factory used to create Las Readers
  */
case class LASPartitionReaderFactory(
    readDataSchema: StructType,
    options: LASOptions
) extends FilePartitionReaderFactory {

  override def buildReader(
      partitionedFile: PartitionedFile
  ): PartitionReader[InternalRow] = {

    val path = getPathWithoutSchemeAndAuthority(partitionedFile.toPath).toString

    // We return a LasPartitionReader according to the "las_reader" parameter
    options.las_reader match {
      case "las4j" => new LAS4jReader(readDataSchema, path)
      case "pdal"  => new LASPdalReader(readDataSchema, path)
    }
  }
}
