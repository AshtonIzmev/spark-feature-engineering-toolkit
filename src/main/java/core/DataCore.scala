package core

import org.apache.spark.sql.SparkSession

object DataCore {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("prototype")
      // Remove the following line when using a K8S cluster
      .master("local")
      .getOrCreate()

}
