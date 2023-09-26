package IO.LAS.DataSourceV1

import com.github.mreutegg.laszip4j.LASReader
import com.github.mreutegg.laszip4j.laslib.LASwriterLAS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

import java.io.{File, PrintStream}

/** @param path
  * @param dataSchema
  * @param context
  * @param params
  */
class LASOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    params: String
) extends OutputWriter
    with Logging {

  private val stream =
    new PrintStream(CodecStreams.createOutputStream(context, new Path(path)))

  val file_test = new LASReader(new File("/home/MBunel/Bureau/pt002561.las"))

  private val header = file_test.getHeader
  private val writer = new LASwriterLAS()

  override def write(row: InternalRow): Unit = ???
  override def close(): Unit = {
    writer.close(true)
  }

  override def path(): String = path
}
