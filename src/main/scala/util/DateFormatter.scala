package util

import constants.GameweekColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, month, to_date, year}

object DateFormatter {

  def timestampToDate(df: DataFrame, newColumn: String, targetColumn: String): DataFrame = {
    df.withColumn(newColumn, to_date(col(targetColumn), "yyyy-MM-dd"))
  }

  def getMonth(df: DataFrame, targetColumn: String): DataFrame = {
    df.withColumn(GameweekColumns.MONTH, month(col(targetColumn)))
  }

  def getYear(df: DataFrame, targetColumn: String): DataFrame = {
    df.withColumn(GameweekColumns.YEAR, year(col(targetColumn)))
  }

  def dateToMonthYear(df: DataFrame, targetColumn: String): DataFrame = {
    val monthDf: DataFrame = getMonth(df, targetColumn)
    getYear(monthDf, targetColumn)
  }
}
