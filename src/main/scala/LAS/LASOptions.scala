package LAS

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.json4s.scalap.Failure.error

import java.time.LocalDate
import java.util.Locale

class LASOptions(@transient parameters: CaseInsensitiveMap[String]) extends FileSourceOptions(parameters) {

  import LASOptions._

  // Day of year of writing, by default current day (in local time)
  val creation_day = parameters.getOrElse(CREATION_DAY, LocalDate.now.getDayOfYear)
  // Year of writing, by default current year
  val creation_year = parameters.getOrElse(CREATION_YEAR, LocalDate.now.getYear)
  // Value of system_id, by default software name + version
  val system_id: String = parameters.getOrElse(SYSTEM_ID, {
    val imp_title = getClass.getPackage.getImplementationTitle
    val imp_version = getClass.getPackage.getImplementationVersion
    s"""$imp_title : $imp_version"""
  })
  // Value of project_id parameter, nul by default
  val project_id = parameters.getOrElse(PROJECT_ID, "")
  val pdal_metadata = getBool(PDAL_METADATA)
  val las_reader: String = parameters.get(LAS_READER) match {
    case _: None.type => "pdal"
    case Some(value) => value match {
      case x if x == "pdal" || x == "las4j" => x
      case _ => throw error
    }
  }

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  private def getBool(paramName: String, default: Boolean = false): Boolean = {
    val param = parameters.getOrElse(paramName, default.toString)
    if (param == null) {
      default
    } else if (param.toLowerCase(Locale.ROOT) == "true") {
      true
    } else if (param.toLowerCase(Locale.ROOT) == "false") {
      false
    } else {
      false
      // QEE is private, find a solution
      //QueryExecutionErrors.paramIsNotBooleanValueError(paramName)
    }
  }

}

object LASOptions extends DataSourceOptions {

  val COMPRESSION = newOption("compression")
  val CREATION_DAY = newOption("creation_day")
  val CREATION_YEAR = newOption("creation_year")
  val SYSTEM_ID = newOption("system_id")
  val PROJECT_ID = newOption("project_id")
  val PDAL_METADATA = newOption("pdal_metadata")
  val LAS_READER = newOption("las_reader")
}