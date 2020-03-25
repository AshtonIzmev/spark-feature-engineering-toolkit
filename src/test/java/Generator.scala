import core.DataCore.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}


object Generator {

  val sparkTypesMap = ("string" -> StringType, "float" -> FloatType, "int" -> IntegerType, "date" -> DateType)

  def getTestDF(rows: Seq[Row]) = {
    val schema = StructType(
      StructField("string", StringType) ::
        StructField("val", FloatType) ::
        StructField("int", IntegerType) ::
        StructField("date1", DateType) ::
        StructField("date2", DateType) ::
        StructField("date3", DateType) :: Nil)
    spark.createDataFrame(spark.sparkContext.makeRDD[Row](rows), schema)
  }

  def getSimpleTestDF(rows: Seq[(String, String)]) = {
    val schema = StructType(
      StructField("string1", StringType) ::
        StructField("string2", StringType) :: Nil)
    spark.createDataFrame(spark.sparkContext.makeRDD[Row](rows.map(s => Row(s._1, s._2))), schema)
  }

}