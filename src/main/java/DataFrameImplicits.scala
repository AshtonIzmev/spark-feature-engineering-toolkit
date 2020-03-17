import java.sql.Date
import java.util.Calendar

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, MetadataBuilder, StringType}


object DataFrameImplicits {

  /**
   * Help renaming agrgegated columns to something more readable
   * @param col : column to rename
   * @param lib : renamed column context
   * @return
   */
  def replaceHeuristic(col: String, lib: String) = {
    val lib2 = if (lib.isEmpty) { "" } else {"_"+lib}
    col
      .replaceAll("sum\\(exist_(\\d+)\\)", "nb" + lib2 + "_$1_mois")
      .replaceAll("sum\\(v_(\\d+)\\)", "mt" + lib2 + "_$1_mois")
      .replaceAll("avg\\(v_(\\d+)\\)", "moy" + lib2 + "_$1_mois")
      .replaceAll("max\\(v_(\\d+)\\)", "max" + lib2 + "_$1_mois")
  }

  implicit class DataFrameImprovements(df: DataFrame) {

    /**
     * rename 'right' multiple columns
     * example :
     * df.renameExt('_right', 'col1', 'col2')
     * will produce
     * df.withColumnRenamed('col1', 'col1_right')
     *   .withColumnRenamed('col2', 'col2_right')
     */
    def renameRight(extension: String, cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, col + extension) }
    }

    /**
     * create multiple columns filled with null
     */
    def createNullCol(cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, col) => df_arg.withColumn(col, lit(null).cast(StringType)) }
    }

    /**
     * rename 'right' multiple columns
     * example :
     * df.renameExt('_right', 'col1', 'col2')
     * will produce
     * df.withColumnRenamed('col1', 'col1_right')
     *   .withColumnRenamed('col2', 'col2_right')
     */
    def renameAllRight(extension: String): DataFrame = {
      df.columns.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, col + extension) }
    }

    /**
     * rename multiple columns using Map function
     * example :
     * df.renameMap("ok" -> "ko", "ik" -> "ki")
     * df.renameMap(Map('ok' -> 'ko', 'ik' -> 'ki'))
     * will produce
     * df.withColumnRenamed('ok', 'ko')
     *   .withColumnRenamed('ik', 'ki')
     */
    def renameMap(renExpr: (String, String), renExprs: (String, String)*): DataFrame = {
      (renExpr +: renExprs).foldLeft(df) { case (df_arg, elem) => df_arg.withColumnRenamed(elem._1, elem._2) }
    }

    /**
     * Same as renameMap((String,String)*) but using a map instead
     */
    def renameMap(map: Map[String, String]): DataFrame = {
      map.foldLeft(df) { case (df_arg, elem) => df_arg.withColumnRenamed(elem._1, elem._2) }
    }

    /**
     * Replace columns that contain a substring
     * @param existingSubstringCol Substring to look for in old columns
     * @param replaceBySubtringCol Replace pattern in old columns
     * @return
     *         Example : if column "abc12ab" exists
     *         Calling the code with parameters "12a" , "NEW"
     *         will rename the column "abc12ab" to "abcNEWb"
     */
    def withColumnSubstringRenamed(existingSubstringCol:String, replaceBySubtringCol:String): DataFrame = {
      df.columns.foldLeft(df) {
        case (df_arg, col) => if (col.contains(existingSubstringCol)) {
          df_arg.withColumnRenamed(col, col.replace(existingSubstringCol, replaceBySubtringCol))
        } else {
          df_arg
        }
      }
    }

    /**
     * Simple renameMap of withColumnSubstringRenamed function :)
     */
    def renameSubstringMap(renExpr: (String, String), renExprs: (String, String)*): DataFrame = {
      (renExpr +: renExprs).foldLeft(df) { case (df_arg, elem) =>
        df_arg.withColumnSubstringRenamed(elem._1, elem._2) }
    }

    /**
     * CF : replaceHeuristic
     */
    def renameExistValueColumns(lib:String=""): DataFrame = {
      df.columns.foldLeft(df) {
        case (df_arg, col) => df_arg.withColumnRenamed(col, replaceHeuristic(col, lib))
      }
    }

    /**
     * Drop columns that contains a certain pattern in a DataFrame
     * @param pattern to exclude from columns
     * @return
     */
    def dropColsContaining(pattern:String): DataFrame = {
      df.columns.filter(_.contains(pattern)).foldLeft(df) {
        case (df_arg, col) => df_arg.drop(col)
      }
    }

    /**
     * Drop multiple columns (more concise syntax)
     * @return
     */
    def drop(columns:Column*): DataFrame = {
      columns.foldLeft(df) {
        case (df_arg, col) => df_arg.drop(col)
      }
    }

    /**
     * rename 'left' multiple columns
     * example :
     * df.renameExt('left_', 'col1', 'col2')
     * will produce
     * df.withColumnRenamed('col1', 'left_col1')
     *   .withColumnRenamed('col2', 'left_col2')
     */
    def renameLeft(extension: String, cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, extension + col) }
    }

    /**
     * Remove a string from column names
     * Used mainly to remove the annoying back tick
     */
    def repString(str: String, repl:String): DataFrame = {
      df.columns.foldLeft(df) { case (df_arg, c) => df_arg.withColumnRenamed(c, c.replace(str, repl)) }
    }

    /**
     * Remove a string from column names
     * Used mainly to remove the annoying back tick
     */
    def remString(str: String): DataFrame = {
      df.repString(str, "")
    }

    /**
     * takes multiple columns names
     * replace ',' with '.'
     * cast to float
     */
    def applyCastToFloat(cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, c) => df_arg.withColumn(c, regexp_replace(col(c), ",", ".")
        .cast(FloatType))
      }
    }

    /**
     * Computes a lazy union, which is a union of two dataframe based
     * on intersection of columns without throwing error if schemas do not match
     */
    def lazyUnion(df_arg:DataFrame): DataFrame = {
      val cols_intersect = df.columns.intersect(df_arg.columns)
      df.select(cols_intersect.map(col): _*).union(df_arg.select(cols_intersect.map(col): _*))
    }

    /**
     * Simple renaming shorter method for readability :)
     */
    def wcr(existingCol:String, newCol:String) = df.withColumnRenamed(existingCol, newCol)

    /**
     * Reduce a month / year based dataframe to a single element per month / year
     * given a baseCalendar (often now)
     * warning : Calendar.MONTH is 0 based
     */
    def projectMonthYearNow(baseCalendar:Calendar, colMonth:String, colYear:String) =
      df.filter(col(colYear) === baseCalendar.get(Calendar.YEAR) &&
        col(colMonth) === 1+baseCalendar.get(Calendar.MONTH))

    /**
     * Reduce a month / year based dataframe to a filtered dataframe
     * given a start and end date
     */
    def borneMonthYear(dateStart:Date, dateEnd:Date, colMonth:String, colYear:String) =
      df.withColumn("date_tmp", to_date(concat_ws("-", col(colYear), col(colMonth), lit("01"))))
        .filter(col("date_tmp") >= lit(dateStart) && col("date_tmp") <= lit(dateEnd))
        .drop("date_tmp")

    /**
     * Given a spark window, we just lag a variable multiple times
     */
    def addLag(w:WindowSpec, colName:String, vals:List[Int]) = {
      vals.foldLeft(df) {
        case (df_arg, v) => df_arg.withColumn(colName + "_lag_" + v.toString, lag(colName, v).over(w))
      }
    }

    /**
     * Simple replacing values in a colum from a Map and leaving unchanged non existant keys in the map
     * @return
     */
    def mapValues(colVal:String, mapVal:Map[String, String]) = {
      mapVal.foldLeft(df) {
        case (df_arg, kv) => df_arg.withColumn(colVal, when(col(colVal) === kv._1, kv._2).otherwise(col(colVal)))
      }
    }

    /**
     * Simple getOrElse spark dataframe easy
     * @param newColName : name of the new created column
     * @param getColName : name of the column we want to get non null values
     * @param elseColName : name of the columns from which values are going to be used if getColName values are null
     */
    def withColumnGetOrElse(newColName:String, getColName:String, elseColName:String) =
      df.withColumn(newColName, when(col(getColName).isNotNull, col(getColName))
        .otherwise(col(elseColName)))

    /**
     * Simple overriding column with condition and litteral
     * @param overrideColName column name to override
     * @param columnCondition condition on which you replace
     * @param  getLit value litteral to replace with
     */
    def overrideColumnCondition(overrideColName:String, columnCondition:Column, getLit:String) =
      df.withColumn(overrideColName, when(columnCondition, getLit).otherwise(col(overrideColName)))

    /**
     * Override a column using multiple conditions
     * @param overrideColName column name to override
     * @param mapConditionLit conditions in a map form
     * @return
     */
    def overrideColumnConditions(overrideColName:String, mapConditionLit:Map[Column, String]) =
      mapConditionLit.foldLeft(df) {
        case (df_arg, kv) => df_arg.overrideColumnCondition(overrideColName, kv._1, kv._2)
      }

    /**
     * Implements an Oracle Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName New column name
     * @param decodeColName Column on which condition will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName and
     *                  _2 : columnName to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found (default empty string)
     * @return
     */
    def withColumnDecodeStringLit(newColName:String, decodeColName:String, decodeMap:Map[String, String],
                                  fallBackCol:Column=lit("")) =
      decodeMap.foldLeft(
        df.withColumn(newColName, fallBackCol)
      ) {
        case (df_arg, kv) => df_arg.withColumn(newColName, when(col(decodeColName) === lit(kv._1), lit(kv._2))
          .otherwise(col(newColName)))
      }

    /**
     * Implements an "Oracle" Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName New column name
     * @param decodeColName Column on which condition will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName and
     *                  _2 : columnName to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found (default zero)
     * @return
     */
    def withColumnDecodeIntLit(newColName:String, decodeColName:String, decodeMap:Map[String, Int],
                               fallBackCol:Column=lit(0)) =
      decodeMap.foldLeft(
        df.withColumn(newColName, fallBackCol)
      ) {
        case (df_arg, kv) => df_arg.withColumn(newColName, when(col(decodeColName) === lit(kv._1), lit(kv._2))
          .otherwise(col(newColName)))
      }

    /**
     * Same as withColumnDecodeIntLit but with a [String,String] decode Map instead of a Int
     * Could be generic
     */
    def withColumnDecodeCol(newColName:String, decodeColName:String, decodeMap:Map[String, String],
                            fallBackCol:Column=lit("")) =
      decodeMap.foldLeft(
        df.withColumn(newColName, fallBackCol)
      ) {
        case (df_arg, kv) => df_arg.withColumn(newColName, when(col(decodeColName) === lit(kv._1), col(kv._2))
          .otherwise(col(newColName)))
      }

  }

}