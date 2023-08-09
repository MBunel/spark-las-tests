package LAS

import com.github.mreutegg.laszip4j.LASReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

import java.io.File

/** Las file reader, based on  laszip4j
  * @param path
  */
class LAS4jReader(path: String) extends PartitionReader[InternalRow] {

  val reader = new LASReader(new File(path))
  val header = reader.getHeader
  val points = reader.getPoints.iterator()

  override def next(): Boolean = points.hasNext

  override def get(): InternalRow = {
    val point = points.next()
    InternalRow(
      point.getX.toFloat * header.getXScaleFactor.toFloat,
      point.getY.toFloat * header.getYScaleFactor.toFloat,
      point.getZ.toFloat * header.getZScaleFactor.toFloat,
      point.getClassification
    )
  }

  override def close(): Unit = {}
}
