package tool.`implicit`

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}
import tool.`implicit`.DataFrameImplicits._


object DataFrameTimeAggImplicits {

  implicit class DataFrameTimeAggImprovements(df: DataFrame) {

    /**
     * Returns a column check_date for each row which satisfies
     * @return  eventDate € [baseDate - b*Month, baseDate - a*Month] for interval = (a, b)
     *          warning (a,b) should satisfie : a < b
     */
    def checkDateMonthInterval(eventDate:String, baseDate:String, interval:(Int, Int)*): DataFrame = {
      interval.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("exist_"+c._1+"_"+c._2,
          (add_months(col(eventDate), c._2) > col(baseDate)
            && add_months(col(eventDate), c._1) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * Returns a column check_date for each row which satisfies
     * @return  eventDate € [baseDate - a*Month, baseDate] for month = a
     */
    def checkDateMonthMilestone(eventDate:String, baseDate:String, months:Int*): DataFrame = {
      months.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("exist_"+c,
          (add_months(col(eventDate), c) >= col(baseDate)
            && col(eventDate) < col(baseDate)).cast(IntegerType)) }
    }

    /**
     * Returns a column val_date for each row which satisfies
     * @return  eventDate € [baseDate - b*Month, baseDate - a*Month] for interval = (a, b)
     *          warning (a,b) should satisfie : a < b
     *          Colunm created contains colVal value
     */
    def valDateMonthInterval(eventDate:String, baseDate:String, colVal:String, lib: String,
                             intervals:(Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn(lib + "_"+c._1+"_"+c._2, col(colVal) *
          (add_months(col(eventDate), c._2) > col(baseDate)
            && add_months(col(eventDate), c._1) < col(baseDate)).cast(IntegerType))
      }
    }

    /**
     * Machine Learning specific
     * Replace interval columns containing zeros with nulls
     */
    def zerosReplaceWithNullsMonthInterval(lib: String, intervals:(Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn(lib+"_"+c._1+"_"+c._2, when(col(lib+"_"+c._1+"_"+c._2) === 0, null)
          .otherwise(col(lib + "_"+c._1+"_"+c._2)))
      }
    }

    /**
     * Machine Learning specific
     * Create variable columns containing an indicator that says an event occured in a particular interval
     * given a baseDate column
     */
    def valDateMonthDoubleInterval(eventDate:String, baseDate:String, colVal:String,
                                   intervals:(Int, Int, Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg
          .withColumn("tmp_"+c._3+"_"+c._4, col(colVal) *
            (add_months(col(eventDate), c._4) > col(baseDate)
              && add_months(col(eventDate), c._3) < col(baseDate)).cast(IntegerType))
          .withColumn("tmp_"+c._1+"_"+c._2, col(colVal) *
            (add_months(col(eventDate), c._2) > col(baseDate)
              && add_months(col(eventDate), c._1) < col(baseDate)).cast(IntegerType))
      }
    }

    /**
     * Returns a column val_date for each row which satisfies
     * @return  eventDate € [baseDate - a*Month, baseDate] for nb_months = a
     *          Colunm created contains colVal value
     */
    def valDateMonthMilestone(eventDate:String, baseDate:String, colVal:String, months:Int*) = {
      months.foldLeft(df) { case (df_arg, c) =>
        df_arg.withColumn("v_"+c, col(colVal) *
          (add_months(col(eventDate), c) >= col(baseDate)
            && col(eventDate) < col(baseDate)).cast(IntegerType)) }
    }


    /**
     * Returns a dataframe with only the first partitionCol element given a sort order : `cols`
     * @param partitionCols Column name used to partition the window, equivalent to a groupBy
     * @param orderCols Columns used to sort each partition (with asc or desc functions)
     * WARNING : Rank can output multiple '1's in case of equality
     *           You should carefully choose the sort order
     */
    def getTop(partitionCols:Column*)(orderCols: Column*) = {
      df
        .withColumn("rank", rank.over(
          Window
            .partitionBy(partitionCols:_*)
            .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") === 1)
    }

    /**
     * Same as getTop but with a howMany tops parameter
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
     * Same as getTop but with a percent parameter instead of a rank one
     */
    def getTopsPercent(partitionCols:Column*)(whichPercent: Double, orderCols: Column*) = {
      df
        .withColumn("rank", percent_rank.over(
          Window
            .partitionBy(partitionCols:_*)
            .orderBy(orderCols :+ rand() :_*)))
        .where(col("rank") <= whichPercent)
    }

    /**
     * Machine Learning specific
     * Feature engineering based on event happenning (dateCol), given a base date (baseDtCol)
     * With a group by sum and a pivot column
     */
    def checkGroupPivotSumRen(dateCol:String, baseDtCol:String,
                              grpByCol:String, pivotCol:String, lib:String, milestones:Seq[Int]) = {
      df
        // kouza3bila means a "random thing" in moroccan arabic
        .transform(conditionnalTransform(milestones.length == 1)(_.withColumn("khouza3bila", lit(1))))
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .transform(conditionnalTransform(milestones.length == 1)(_.dropColsContaining("khouza3bila")))
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Machine Learning specific
     * Same as checkGroupPivotSumRen but with ratio calculations instead of "event exist" features
     */
    def checkRatioValGroupPivotRen(dateCol:String, baseDtCol:String, colVal:String,
                                   grpByCol:String, pivotCol:String, lib:String, c:Seq[(Int, Int, Int, Int)]) = {
      df
        .transform(conditionnalTransform(c.length == 1)(_.withColumn("khouza3bila", lit(1))))
        .valDateMonthDoubleInterval(dateCol, baseDtCol, colVal, c:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .ratioFoldCalulation(c:_*)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .transform(conditionnalTransform(c.length == 1)(_.dropColsContaining("khouza3bila")))
        .dropColsContaining("tmp")
        .dropColsContaining("sum(sum")
        .remString("sum")

    }

    /**
     * Machine Learning specific
     */
    def checkRatioValGroupRen(dateCol:String, baseDtCol:String, colVal:String,
                              grpByCol:String, pivotCol:String, lib:String, c:Seq[(Int, Int, Int, Int)]) = {
      df
        .valDateMonthDoubleInterval(dateCol, baseDtCol, colVal, c:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .ratioFoldCalulation(c:_*)
        .dropColsContaining("tmp")
        .remString("sum")
    }

    def ratioFoldCalulation(intervals:(Int, Int, Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg
          .withColumn("mt_"+c._1+"_"+c._2+"_"+c._3+"_"+c._4 +"_ratio",
            log((col("sum("+"tmp_"+c._1+"_"+c._2+")")+1)/ (col("sum("+"tmp_"+c._3+"_"+c._4+")")+1)))
      }
    }

    def checkGroupSumRen(dateCol:String, baseDtCol:String,
                         grpByCol:String, pivotCol:String, lib:String, milestones:Seq[Int]) = {
      df
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .renameColumnsWithAggHeuristic(lib)
    }

    def checkValGroupPivotSumRen(dateCol:String, baseDtCol:String, valCol:String,
                                 grpByCol:String, pivotCol:String, lib:String, milestones:Seq[Int]) = {
      df
        .transform(conditionnalTransform(milestones.length == 1)(_.withColumn("khouza3bila", lit(1))))
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .valDateMonthMilestone(dateCol, baseDtCol, valCol, milestones:_*)
        .drop(valCol)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .transform(conditionnalTransform(milestones.length == 1)(_.dropColsContaining("khouza3bila")))
        .renameColumnsWithAggHeuristic(lib)
    }

    def checkValGroupSumRen(dateCol:String, baseDtCol:String, valCol:String,
                            grpByCol:String, pivotCol:String, lib:String, milestones:Seq[Int]) = {
      df
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .valDateMonthMilestone(dateCol, baseDtCol, valCol, milestones:_*)
        .drop(valCol)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .renameColumnsWithAggHeuristic(lib)
    }


    def conditionnalTransform(condition:Boolean)(dataFrameTrans: DataFrame => DataFrame)
                             (df_arg:DataFrame) : DataFrame = {
      if (condition) return df_arg.transform(dataFrameTrans)
      df_arg
    }

  }

}
