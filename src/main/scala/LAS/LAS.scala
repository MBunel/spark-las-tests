package LAS

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Class Las
  *
  * Main class for the las datasource.
  *
  * Inspired by the spark csv and parquet readers
  *
  * TODO: Schema inference; write las; filters
  */
class LAS extends FileDataSourceV2 {

  /** @return
    */
  override def fallbackFileFormat: Class[_ <: FileFormat] = null

  /** @param options
    * @return
    */
  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    LASTable(
      tableName,
      sparkSession,
      optionsWithoutPaths,
      paths,
      None,
      fallbackFileFormat
    )
  }

  /** @return
    */
  override def shortName(): String = "las"

  /** @return
    */
  override def supportsExternalMetadata(): Boolean = true
}
