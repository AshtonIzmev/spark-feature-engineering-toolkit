package tool

import core.DataCore
import core.DataCore.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoading {

  def loadCsv(filename:String, separator:String=",", inferSchema: String="true"): DataFrame = {
    DataCore.spark.read.format("csv")
      .option("sep", separator)
      .option("inferSchema", inferSchema)
      .option("header", "true")
      .load(filename)
  }

}
