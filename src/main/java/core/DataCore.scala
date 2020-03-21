package core

import org.apache.spark.sql.SparkSession

object DataCore {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("prototype")
      .master("local")
      .getOrCreate()

}
