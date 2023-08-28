package LAS

import io.pdal.PointViewIterator
import io.pdal.pipeline.ReadLas
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.net.URI

/** Las file reader, based on pdal
  * @param path
  */
class LASPdalReader(conf: Configuration, readDataSchema: StructType, path: URI)
    extends PartitionReader[InternalRow] {

  private val expression = ReadLas(path.getPath)
  private val pipeline = expression.toPipeline
  pipeline.initialize()
  pipeline.execute()

  private val pvs: PointViewIterator = pipeline.getPointViews()
  private val pv = pvs.next()

  private val points_count = pv.length()
  private var counter = 0

  override def next(): Boolean = this.counter < this.points_count

  override def get(): InternalRow = {
    val row = InternalRow(
      pv.getX(this.counter).toFloat,
      pv.getY(this.counter).toFloat,
      pv.getY(this.counter).toFloat,
      pv.getShort(this.counter, "Classification")
    )
    this.counter += 1
    row
  }

  override def close(): Unit = pvs.close()
}
