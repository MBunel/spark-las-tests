package LAS.DataSourceV1

import com.github.mreutegg.laszip4j.LASReader
import com.github.mreutegg.laszip4j.laslib.{LASheader, LASwriterLAS}
import com.github.mreutegg.laszip4j.laszip.{
  LASattributer,
  LASitem,
  LASpoint,
  LASquantizer
}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

import java.io.{File, PrintStream}

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

  private val header = file_test.getHeader.hea
  private val writer = new LASwriterLAS()

  writer.open(stream, header, '0', 0, -1)
  override def write(row: InternalRow): Unit = {
    val pt: LASpoint = new LASpoint()
    val qt = new LASquantizer()
    val at = new LASattributer()

    pt.init(qt, 0, new LASitem(), at)

    writer.write_point(pt)
  }

  override def close(): Unit = {
    writer.update_header(header, true, true)
    writer.close(true)
  }

  override def path(): String = path
}
