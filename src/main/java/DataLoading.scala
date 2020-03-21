import core.DataCore
import org.apache.spark.sql.DataFrame

object DataLoading {

  def loadCsv(filename:String, separator:String=","): DataFrame = {
    DataCore.spark.read.format("csv")
      .option("sep", separator)
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)
  }

}
