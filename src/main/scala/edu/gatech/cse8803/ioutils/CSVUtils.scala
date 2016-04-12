/**
  * @author Joshua Powers <powersj@gatech.edu>.
  * @author Hang Su <hangsu@gatech.edu>.
  */
package edu.gatech.cse8803.ioutils

import com.databricks.spark.csv.CsvContext
import org.apache.spark.sql.{DataFrame, SQLContext}


object CSVUtils {
  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored

  def loadCSVAsTable(sqlContext: SQLContext, path: String): DataFrame = {
    loadCSVAsTable(sqlContext, path, inferTableNameFromPath(path))
  }

  def loadCSVAsTable(sqlContext: SQLContext, path: String, tableName: String): DataFrame = {
    val data = sqlContext.csvFile(path)
    data.registerTempTable(tableName)
    data
  }

  def inferTableNameFromPath(path: String) = path match {
    case pattern(filename, extension) => filename
    case _ => path
  }
}
