package IO

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object LAS {
  implicit class LASDataFrameReader(reader: DataFrameReader) {

    /** Add a method "las" to DataFrameReader that allows you to read LAS files using
      * the DataFileReader
      */

    def las: String => DataFrame =
      reader.format("IO.LAS.LAS").load

    // TODO Find solution for multi path
    //def las: (String*) => DataFrame =
    //   reader.format("IO.LAS.LAS").load
  }
}
