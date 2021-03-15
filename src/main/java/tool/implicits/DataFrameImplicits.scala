package tool.implicits

import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{Column, DataFrame}

import java.sql.Date
import java.util.Calendar


object DataFrameImplicits {

  /**
   * Help renaming agrgegated columns to something more readable
   * "exist" is an indicator that tells if a value has been found.
   * It is build with a .withColumn(booleanColumn.cast(IntegerType))
   * "v" is the numeric value
   * @param col : column to rename
   * @param lib : renamed column context (transfer, deposit, call etc.)
   * @example Column "sum(exist_6)" will be converted to "nb_6_month"
   *          Column "avg(v_24)" will be converted to "avg_24_month"
   * @return
   */
  def replaceAggFunctionsHeuristic(col: String, lib: String) = {
    val lib2 = if (lib.isEmpty) { "" } else {"_"+lib}
    col
      .replaceAll("sum\\(exist_(\\d+)\\)", "nb" + lib2 + "_$1_month")
      .replaceAll("sum\\(v_(\\d+)\\)", "sum" + lib2 + "_$1_month")
      .replaceAll("avg\\(v_(\\d+)\\)", "avg" + lib2 + "_$1_month")
      .replaceAll("max\\(v_(\\d+)\\)", "max" + lib2 + "_$1_month")
  }

  implicit class DataFrameImprovements(df: DataFrame) {

    /**
     * rename 'right' multiple column names
     * @param extension the right extension to add
     * @param cols the columns to change
     * @return A renamed DataFrame
     * @example df.renameExt('_right', 'col1', 'col2')
     *          will produce
     *          df.withColumnRenamed('col1', 'col1_right')
     *            .withColumnRenamed('col2', 'col2_right')
     */
    def renameRight(extension: String, cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, col + extension) }
    }

    /**
     * The same function as renameRight but applied on all the DataFrame columns
     * @see renameRight(extension: String, cols: String*)
     */
    def renameAllRight(extension: String): DataFrame = {
      df.columns.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, col + extension) }
    }

    /**
     * Rename multiple columns using Map function
     * @param renExpr the couple "a" -> "b" that defines the input and output column
     * @return A renamed DataFrame
     * @example df.renameMap("ok" -> "ko", "ik" -> "ki")
     *          will produce
     *          df.withColumnRenamed('ok', 'ko')
     *            .withColumnRenamed('ik', 'ki')
     */
    def renameMap(renExpr: (String, String), renExprs: (String, String)*): DataFrame = {
      (renExpr +: renExprs).foldLeft(df) { case (df_arg, elem) => df_arg.withColumnRenamed(elem._1, elem._2) }
    }

    /**
     * Same as renameMap((String,String)*) but using a map instead
     * @see renameMap(renExpr: (String, String), renExprs: (String, String)*)
     */
    def renameMap(map: Map[String, String]): DataFrame = {
      map.foldLeft(df) { case (df_arg, elem) => df_arg.withColumnRenamed(elem._1, elem._2) }
    }

    /**
     * Replace columns that contain a specific substring
     * @param existingSubStrColName Substring to look for in input columns
     * @param replaceBySubStrColName Replace pattern in output columns
     * @return
     *         Example : if column "abc12ab" exists
     *         Calling the code with parameters "12a" , "NEW"
     *         will rename the column "abc12ab" to "abcNEWb"
     */
    def withColumnSubStrRenamed(existingSubStrColName:String, replaceBySubStrColName:String): DataFrame = {
      df.columns.filter(_.contains(existingSubStrColName)).foldLeft(df) {
        case (df_arg, col) =>
          df_arg.withColumnRenamed(col, col.replace(existingSubStrColName, replaceBySubStrColName))
      }
    }

    /**
     * Same as renameMap of withColumnSubstringRenamed function
     * @see withColumnSubStrRenamed(existingSubStrColName:String, replaceBySubStrColName:String)
     */
    def renameSubStrMap(renExpr: (String, String), renExprs: (String, String)*): DataFrame = {
      (renExpr +: renExprs).foldLeft(df) { case (df_arg, elem) =>
        df_arg.withColumnSubStrRenamed(elem._1, elem._2) }
    }

    /**
     * Uses the replaceAggFunctionsHeuristic to change the name of columns
     * @param lib renamed column context (transfer, deposit, call etc.)
     * @return the same dataframe with the replaced columns
     */
    def renameColumnsWithAggHeuristic(lib:String=""): DataFrame = {
      df.columns.foldLeft(df) {
        case (df_arg, col) => df_arg.withColumnRenamed(col, replaceAggFunctionsHeuristic(col, lib))
      }
    }

    /**
     * Drop columns that contains a specific pattern in a DataFrame
     * @param pattern used to drop columns
     * @return the same dataframe without the matched columns
     */
    def dropColsContaining(pattern:String): DataFrame = {
      df.columns.filter(_.contains(pattern)).foldLeft(df) {
        case (df_arg, col) => df_arg.drop(col)
      }
    }

    /**
     * Drop multiple columns (more concise syntax)
     * @param cols columns to drop
     * @return the same dataframe with less columns
     */
    def drop(cols:Column*): DataFrame = {
      cols.foldLeft(df) {
        case (df_arg, col) => df_arg.drop(col)
      }
    }

    /**
     * Rename multiple columns with a suffix (left string extension)
     * @param extension for example '_left'
     * @param cols columns that will have the extension added
     * @return The same dataframe with renamed columns
     * @example df.renameExt('left_', 'col1', 'col2')
     *          will produce
     *          df.withColumnRenamed('col1', 'left_col1')
     *            .withColumnRenamed('col2', 'left_col2')
     */
    def renameLeft(extension: String, cols: String*): DataFrame = {
      cols.foldLeft(df) { case (df_arg, col) => df_arg.withColumnRenamed(col, extension + col) }
    }

    /**
     * Remove a pattern from column names. Used mainly to remove the annoying back tick when using special characters
     * @param pattern to use for the replace function
     * @param repl used to replace
     * @return the same dataframe with new column name
     */
    def repString(pattern: String, repl:String): DataFrame = {
      df.columns.foldLeft(df) { case (df_arg, c) => df_arg.withColumnRenamed(c, c.replace(pattern, repl)) }
    }

    /**
     * Remove a string for column names
     * @see repString(pattern: String, repl:String)
     */
    def remString(str: String): DataFrame = {
      df.repString(str, "")
    }

    /**
     * Replace ',' with '.' and then cast to float to convert string columns to float ones
     * @param cols columns to cast
     * @return the same dataframe with float columns
     */
    def applyCastToFloat(cols: String*): DataFrame = {
      cols.foldLeft(df) {
        case (df_arg, c) => df_arg
          .withColumn(c, regexp_replace(col(c), ",", ".").cast(FloatType))
      }
    }

    /**
     * Computes a lazy union, which is a union of two dataframe based
     * on intersection of columns without throwing error if schemas do not match.
     * Used when the input dataframes columns are not controlled (for example when parsing a csv file)
     * @param df_arg the other dataframe to union
     * @return the lazy union of both dataframes
     */
    def lazyUnion(df_arg:DataFrame): DataFrame = {
      val cols_intersect = df.columns.intersect(df_arg.columns)
      df.select(cols_intersect.map(col): _*).union(df_arg.select(cols_intersect.map(col): _*))
    }

    /**
     * Simple renaming shorter method for readability
     * @see withColumnRenamed(existingName: String, newName: String)
     */
    def wcr(existingName:String, newName:String) = df.withColumnRenamed(existingName, newName)

    /**
     * Filter a month / year based dataframe  given a baseCalendar (often now)
     * @note  Calendar.MONTH is 0 based (we add the 1+ in the filtering process)
     * @param baseCalendar java.util.Calendar to use for month/year filtering
     * @param colMonth column corresponding to the month
     * @param colYear column correspond to the year
     * @return A filtered dataframe
     */
    def projectMonthYearNow(baseCalendar:Calendar, colMonth:String, colYear:String) =
      df.filter(col(colYear) === baseCalendar.get(Calendar.YEAR) &&
        col(colMonth) === 1+baseCalendar.get(Calendar.MONTH))

    /**
     * Filter a month / year based dataframe  given a start and end date
     * @note Calendar.MONTH is 0 based (we add the 1+ in the filtering process)
     * @note We assume the dataframe day is 01
     * @param dateStart java.sql.Date start date
     * @param dateEnd java.sql.Date end date
     * @param colMonth column corresponding to the month
     * @param colYear column correspond to the year
     * @return A filtered dataframe
     */
    def borneMonthYear(dateStart:Date, dateEnd:Date, colMonth:String, colYear:String) =
      df.withColumn("date_tmp", to_date(concat_ws("-", col(colYear), col(colMonth), lit("01"))))
        .filter(col("date_tmp") >= lit(dateStart) && col("date_tmp") <= lit(dateEnd))
        .drop("date_tmp")

    /**
     * Given a spark window, we just lag a variable multiple times
     * @param w the spark window to use
     * @param colName the lagged column
     * @param vals the integer lag values
     * @return a dataframe with new lagged columns
     */
    def addLag(w:WindowSpec, colName:String, vals:List[Int]) = {
      vals.foldLeft(df) {
        case (df_arg, v) => df_arg.withColumn(colName + "_lag_" + v.toString, lag(colName, v).over(w))
      }
    }

    /**
     * Replacing values in a colum from a Map and leaving unchanged non existant keys in the map
     * Similar to Oracle decode function but with no default value
     * @param colToDecode column used to apply the decode
     * @param mapDecode map with key and values to replace by
     * @return a dataframe with new values in colToDecode column
     * @note there may be a more effecient way to do it whith an udf rather than "for" loop
     */
    def mapValues(colToDecode:String, mapDecode:Map[String, String]) = {
      mapDecode.foldLeft(df) {
        case (df_arg, kv) =>
          df_arg.withColumn(colToDecode, when(col(colToDecode) === kv._1, kv._2).otherwise(col(colToDecode)))
      }
    }

    /**
     * Simple getOrElse spark dataframe
     * @param newColName name of the new created column
     * @param getColName name of the column we want to get non null values
     * @param elseColName name of the columns from which values are going to be used if getColName values are null
     * @return a dataframe with a new getOrNull column
     */
    def withColumnGetOrElse(newColName:String, getColName:String, elseColName:String) =
      df.withColumn(newColName, when(col(getColName).isNotNull, col(getColName))
        .otherwise(col(elseColName)))

    /**
     * Simply overwriting column with a boolean condition
     * @param overrideColName name of the new created column
     * @param conditionBoolCol boolean column used as a condition on which you replace
     * @param strIfCond value litteral to replace with
     * @return If conditionBoolCol, then replace with strIfCond, else still use overrideColName
     */
    def overwriteColumnCondition(overrideColName:String, conditionBoolCol:Column, strIfCond:String) =
      df.withColumn(overrideColName, when(conditionBoolCol, strIfCond).otherwise(col(overrideColName)))

    /**
     * Override a column using multiple conditions
     * @param overrideColName column name to override
     * @param mapConditionLit conditions in a map with a conditionBoolCol as a key and strIfConf as a value
     * @see overwriteColumnCondition(overrideColName:String, conditionBoolCol:Column, strIfCond:String)
     */
    def overrideColumnConditions(overrideColName:String, mapConditionLit:Map[Column, String]) =
      mapConditionLit.foldLeft(df) {
        case (df_arg, kv) => df_arg.overwriteColumnCondition(overrideColName, kv._1, kv._2)
      }

    /**
     * Implements an Oracle Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName new column name
     * @param decodeColName column on which decode will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName
     *                  _2 : litteral '''string''' to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found
     * @return a new column with a decoded column
     */
    def withColumnDecodeStringLit(newColName:String, decodeColName:String, decodeMap:Map[String, String],
                                  fallBackCol:Column=lit("")) =
      decodeMap.foldLeft(
        df.withColumn(newColName, fallBackCol)) {
        case (df_arg, kv) =>
          df_arg.withColumn(newColName, when(col(decodeColName) === lit(kv._1), lit(kv._2))
            .otherwise(col(newColName)))
      }

    /**
     * Implements an Oracle Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName new column name
     * @param decodeColName column on which decode will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName
     *                  _2 : litteral '''int''' to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found
     * @return a new column with a decoded column
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
     * Implements an Oracle Decode in spark using a Map of "switch case conditions". Null values are not handled
     * @param newColName new column name
     * @param decodeColName column on which decode will be applied
     * @param decodeMap contains
     *                  _1 : value to be tested against decodeColName
     *                  _2 : column name indicating the colum to be used if the previous test has matched
     * @param fallBackCol column value used when no match has been found
     * @return a new column with a decoded column
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