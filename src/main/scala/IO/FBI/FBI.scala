package IO.FBI

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Dummy implementation of terrascan internal format
 *
 */
class FBI extends FileDataSourceV2 {
  override def fallbackFileFormat: Class[_ <: FileFormat] = ???

  override protected def getTable(options: CaseInsensitiveStringMap): Table =
    ???

  override def shortName(): String = "fbi"
}
