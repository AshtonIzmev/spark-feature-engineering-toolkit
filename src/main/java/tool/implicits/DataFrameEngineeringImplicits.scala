package tool.implicits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import tool.implicits.DataFrameImplicits.DataFrameImprovements
import tool.implicits.DataFrameTimeAggImplicits.DataFrameTimeAggImprovements


/**
 * Those are the functions used for feature engineering
 */
object DataFrameEngineeringImplicits {

  implicit class DataFrameEngineeringImprovements(df: DataFrame) {

    /**
     * Feature engineering function based on event happenning (dateCol), given a base date (baseDtCol)
     * With a group by that includes a pivot category
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column also used to group by
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return exist features for an event DataFrame
     */
    def getCheckFeatures(dateCol:String, baseDtCol:String,
                         grpByCol:String, pivotCol:String,
                         lib:String, milestones:Seq[Int]) = {
      df
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Feature engineering function based on event happenning (dateCol) and an associated value (valCol),
     * given a base date (baseDtCol) with a group by then sum
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column also used to group by
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return exist/value features for an event DataFrame
     */
    def getCheckValueFeatures(dateCol:String, baseDtCol:String, valCol:String,
                              grpByCol:String, pivotCol:String,
                              lib:String, milestones:Seq[Int]) = {
      df
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .valDateMonthMilestone(dateCol, baseDtCol, valCol, "v", milestones:_*)
        .drop(valCol)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Feature engineering function based on event happenning (dateCol), given a base date (baseDtCol)
     * With a group by then sum given a pivot column
     * @note the pivot() is adding new column names with the 'pivotCol' component when there is only one column
     *       hence the khouza3bila fake column to force it add the 'pivotCol'
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column used to group by sum pivot
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return exist pivoted features for an event DataFrame
     */
    def getCheckPivotFeatures(dateCol:String, baseDtCol:String,
                              grpByCol:String, pivotCol:String,
                              lib:String, milestones:Seq[Int]) = {
      df
        // khouza3bila (خزعبلة) means a "random/funny thing" in moroccan arabic
        .conditionnalTransform(milestones.length == 1)(_.withColumn("khouza3bila", lit(1)))
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .conditionnalTransform(milestones.length == 1)(_.dropColsContaining("khouza3bila"))
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Feature engineering function based on event happenning (dateCol) and an associated value (valCol),
     * given a base date (baseDtCol) with a group by then sum given a pivot column
     * @note the pivot() is adding new column names with the 'pivotCol' component when there is only one column
     *       hence the khouza3bila fake column to force it add the 'pivotCol'
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column used to group by sum pivot
     * @param lib a small text included in newly created column names
     * @param milestones used to determine the event date check intervals
     * @return exist/value pivoted features for an event DataFrame
     */
    def getCheckValuePivotFeatures(dateCol:String, baseDtCol:String, valCol:String,
                                   grpByCol:String, pivotCol:String, lib:String, milestones:Seq[Int]) = {
      df
        .conditionnalTransform(milestones.length == 1)(_.withColumn("khouza3bila", lit(1)))
        .checkDateMonthMilestone(dateCol, baseDtCol, milestones:_*)
        .valDateMonthMilestone(dateCol, baseDtCol, valCol, "v", milestones:_*)
        .drop(valCol)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .conditionnalTransform(milestones.length == 1)(_.dropColsContaining("khouza3bila"))
        .renameColumnsWithAggHeuristic(lib)
    }

    /**
     * Calculate a log ratio between two intervals
     * @param lib usually tmp
     * @param intervals intervals used for calculating the base variables
     * @return a dataframe with a new ratio column
     */
    def getRatioFoldCalulation(lib:String, intervals:(Int, Int, Int, Int)*) = {
      intervals.foldLeft(df) { case (df_arg, c) =>
        df_arg
          .withColumn("ratio_"+c._1+"_"+c._2+"_"+c._3+"_"+c._4,
            log((col("sum("+ lib + "_"+c._1+"_"+c._2+")")+1)/ (col("sum("+ lib + "_"+c._3+"_"+c._4+")")+1)))
      }
    }

    /**
     * Build a ratio features dataframe base on multiple double intervals and an event based source dataframe
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column also used to group by
     * @param intervals the double intervals used to calculate the ratio
     * @return a dataframe with the new ratios
     */
    def getRatioFeatures(dateCol:String, baseDtCol:String, valCol:String,
                         grpByCol:String, pivotCol:String,
                         intervals:Seq[(Int, Int, Int, Int)]) = {
      df
        .valDateMonthDoubleInterval(dateCol, baseDtCol, valCol, "tmp", intervals:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .getRatioFoldCalulation("tmp", intervals:_*)
        .dropColsContaining("tmp")
        .remString("sum")
    }

    /**
     * Build a ratio features dataframe base on multiple double intervals and an event based source dataframe
     * @param dateCol event date column
     * @param baseDtCol base date reference column
     * @param valCol event value colum
     * @param grpByCol column used to group by the dataframe. Sum aggregation will be applied
     * @param pivotCol column used to group by sum pivot
     * @param intervals the double intervals used to calculate the ratio
     * @return a pivoted dataframe with the new ratios
     */
    def getRatioPivotFeatures(dateCol:String, baseDtCol:String, valCol:String,
                              grpByCol:String, pivotCol:String,
                              intervals:Seq[(Int, Int, Int, Int)]) = {
      df
        .conditionnalTransform(intervals.length == 1)(_.withColumn("khouza3bila", lit(1)))
        .valDateMonthDoubleInterval(dateCol, baseDtCol, valCol, "tmp", intervals:_*)
        .groupBy(grpByCol, pivotCol)
        .sum()
        .getRatioFoldCalulation("tmp", intervals:_*)
        .groupBy(grpByCol)
        .pivot(pivotCol)
        .sum()
        .conditionnalTransform(intervals.length == 1)(_.dropColsContaining("khouza3bila"))
        .dropColsContaining("tmp").dropColsContaining("sum(sum")
        .remString("sum").remString("(").remString(")")
    }

  }

}
