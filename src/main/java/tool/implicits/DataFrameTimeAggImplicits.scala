package tool.implicits

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}


object DataFrameTimeAggImplicits {

  implicit class DataFrameTimeAggImprovements(df: DataFrame) {

    /**
     * Returns a new column 0/1 exist_a_b for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate - a*Month]
     * given an (a, b) interval
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param intervals (a,b) used to build the [baseDate - b*Month, baseDate - a*Month] interval
     * @note warning (a,b) should satisfie : a < b
     */
    def checkDateMonthInterval(eventDateCol:String, baseDate:String, intervals:(Int, Int)*): DataFrame = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("exist_" + c._1 + "_" + c._2,
          (add_months(col(eventDateCol), c._2) > col(baseDate)
            && add_months(col(eventDateCol), c._1) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * Returns a new column 0/1 exist_b for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate] given a number of months b
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param monthsNb b used to build the [baseDate - b*Month, baseDate] interval
     */
    def checkDateMonthMilestone(eventDateCol:String, baseDate:String, monthsNb:Int*): DataFrame = {
      monthsNb.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("exist_" + c,
          (add_months(col(eventDateCol), c) >= col(baseDate)
            && col(eventDateCol) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * Returns a new column lib_a_b that contains the value of valColName for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate - a*Month]
     * given an (a, b) interval
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param valColName the value used to build the new column
     * @param lib a prefix for the column name
     * @param intervals (a,b) used to build the [baseDate - b*Month, baseDate - a*Month] interval
     * @note warning (a,b) should satisfie : a < b
     */
    def valDateMonthInterval(eventDateCol:String, baseDate:String, valColName:String, lib: String,
                             intervals:(Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn(lib + "_" + c._1 + "_" + c._2, col(valColName) *
          (add_months(col(eventDateCol), c._2) > col(baseDate)
            && add_months(col(eventDateCol), c._1) < col(baseDate)).cast(IntegerType))
      }
    }

    /**
     * Returns a new column lib_a_b that contains the value of valColName for each row which satisfies
     * eventDate € [baseDate - b*Month, baseDate]
     * given an (a, b) interval
     * @param eventDateCol column used as the eventDate
     * @param baseDate column used as a date reference for the row
     *                 (each row may have its own reference when doing feature engineering)
     * @param valColName the value used to build the new column
     * @param lib a prefix for the column name
     * @param monthsNb b used to build the [baseDate - b*Month, baseDate] interval
     * @note warning (a,b) should satisfie : a < b
     */
    def valDateMonthMilestone(eventDateCol:String, baseDate:String, valColName:String, lib: String,
                              monthsNb:Int*) = {
      monthsNb.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn(lib + "_" + c, col(valColName) *
          (add_months(col(eventDateCol), c) >= col(baseDate)
            && col(eventDateCol) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * A way to use two intervals in the same time for building ratio features
     * @see valDateMonthInterval(eventDateCol:String, baseDate:String, valColName:String, lib: String,
     *      intervals:(Int, Int)*)
     */
    def valDateMonthDoubleInterval(eventDate:String, baseDate:String, colVal:String,
                                   lib:String, doubleIntervals:(Int, Int, Int, Int)*) = {
      df
        .valDateMonthInterval(eventDate, baseDate, colVal, lib, doubleIntervals.map(i => (i._1, i._2)):_*)
        .valDateMonthInterval(eventDate, baseDate, colVal, lib, doubleIntervals.map(i => (i._3, i._4)):_*)
    }


    /**
     * Returns a dataframe with only the first partitionCol element given a sort order orderCols
     * @param partitionCols columns used to partition the window, equivalent to a groupBy
     * @param orderCols columns used to sort each partition (with asc or desc functions)
     * @note the rand() function is used in case orderCols in empty. There may be a better way
     * @example df.getTop(col("client_id"))(desc("purchaseDate"))
     *          will return one of the latest purchases for each client
     */
    def getTop(partitionCols:Column*)(orderCols: Column*) = {
      df.withColumn("rank", rank.over(
        Window
          .partitionBy(partitionCols:_*)
          .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") === 1)
        .drop("rank")
    }

    /**
     * Returns a dataframe with only the first partitionCol element given a sort order orderCols
     * @param partitionCols columns used to partition the window, equivalent to a groupBy
     * @param orderCols columns used to sort each partition (with asc or desc functions)
     * @param howMany the number of top elements we want
     * @note the rand() function is used in case orderCols in empty. There may be a better way
     * @example df.getTops(col("client_id"))(5, desc("purchaseDate"), asc("itemPrice"))
     *          will return the 5 last/costly purchases for each client
     */
    def getTops(partitionCols:Column*)(howMany: Int, orderCols: Column*) = {
      df
        .withColumn("rank", rank.over(
          Window
            .partitionBy(partitionCols:_*)
            .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") <= howMany)
    }

    /**
     * Returns a dataframe with only the first partitionCol element given a sort order orderCols
     * @param partitionCols columns used to partition the window, equivalent to a groupBy
     * @param orderCols columns used to sort each partition (with asc or desc functions)
     * @param percent the percentage of top elements we want
     * @note the rand() function is used in case orderCols in empty. There may be a better way
     * @example df.getTopsPercent(col("client_id"))(0.05, desc("purchaseDate"), asc("itemPrice"))
     *          will return the 5% last/costly purchases for each client
     */
    def getTopsPercent(partitionCols:Column*)(percent: Double, orderCols: Column*) = {
      df
        .withColumn("rank", percent_rank.over(
          Window
            .partitionBy(partitionCols:_*)
            .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") <= percent)
    }


    /**
     * A simple if implicit if-else :p
     * @param condition if(condition)
     * @param dataFrameTrans then apply(dataFrameTrans)
     * @return if (condition) transform(df) else df
     */
    def conditionnalTransform(condition:Boolean)(dataFrameTrans: DataFrame => DataFrame) : DataFrame = {
      if (condition) df.transform(dataFrameTrans) else df
    }
  }

}
