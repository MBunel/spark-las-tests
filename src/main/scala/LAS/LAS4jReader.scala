package LAS

import com.github.mreutegg.laszip4j.{LASPoint, LASReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.io.File

/** Las file reader, based on  laszip4j
  * @param path
  */
class LAS4jReader(readDataSchema: StructType, path: String)
    extends PartitionReader[InternalRow] {

  val dataSchema = readDataSchema
  val reader = new LASReader(new File(path))
  val header = reader.getHeader
  val points = reader.getPoints.iterator()

  override def next(): Boolean = points.hasNext

  override def get(): InternalRow = {
    val point = points.next()
    extract_field(point)
  }

  override def close(): Unit = {}

  private def extract_field(point: LASPoint): InternalRow = {
    var fields_values: Seq[Any] = Seq()

    for (field <- this.dataSchema.iterator) {

      val value = field.name match {
        case "X"                   => point.getX.toFloat * header.getXScaleFactor.toFloat
        case "Y"                   => point.getY.toFloat * header.getYScaleFactor.toFloat
        case "Z"                   => point.getZ.toFloat * header.getZScaleFactor.toFloat
        case "Intensity"           => point.getIntensity.toShort
        case "Return number"       => point.getReturnNumber
        case "Number of returns"   => point.getNumberOfReturns
        case "Scan direction flag" => point.getScanDirectionFlag
        case "Edge of flight line" => point.getEdgeOfFlightLine
        case "Classification"      => point.getClassification
        case "Is Synthetic"        => point.isSynthetic
        case "Is key-point"        => point.isKeyPoint
        case "Is Withheld"         => point.isWithheld
        case "Is Overlap"          => null
        case "Scanner Channel"     => null
        case "User data"           => point.getUserData
        case "Scan angle rank"     => point.getScanAngleRank.toShort
        case "Point source ID"     => point.getPointSourceID.toInt
        case "GPS Time"            => point.getGPSTime
      }
      fields_values = fields_values :+ value
    }
    InternalRow.fromSeq(fields_values)
  }

}
