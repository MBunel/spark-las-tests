package LAS

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

/** Non implemented yet
  * @param path
  * @param dataSchema
  * @param context
  * @param LASOptions
  */
case class LASOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    LASOptions: LASOptions
) extends OutputWriter
    with Logging {

  override def write(row: InternalRow): Unit = ???

  override def close(): Unit = ???
}
