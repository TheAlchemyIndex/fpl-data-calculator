package gameweek

import constants.{CommonColumns, GameweekColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, month, to_date, year}

object DateFormatter {

  def timestampToDate(df: DataFrame): DataFrame = {
    df
      .withColumn(CommonColumns.DATE, to_date(col(GameweekColumns.KICKOFF_TIME), "yyyy-MM-dd"))
  }

  def dateToMonthAndYear(df: DataFrame): DataFrame = {
    df
      .withColumn(GameweekColumns.MONTH, month(col(CommonColumns.DATE)))
      .withColumn(GameweekColumns.YEAR, year(col(CommonColumns.DATE)))
  }

  def formatDate(df: DataFrame): DataFrame = {
    val dateDf = timestampToDate(df)
    val monthYearDf = dateToMonthAndYear(dateDf)
    monthYearDf
  }
}
