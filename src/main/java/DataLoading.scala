import org.apache.spark.sql.DataFrame

object DataLoading {

  def loadCsv(filename:String): DataFrame = {
    DataCore.spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filename)
  }

}
