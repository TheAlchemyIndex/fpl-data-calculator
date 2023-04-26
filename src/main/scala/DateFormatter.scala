import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, month, to_date, year}

object DateFormatter {

  def timestampToDate(df: DataFrame): DataFrame = {
    df.withColumn("date",
      to_date(col("kickoff_time"), "yyyy-MM-dd"))
  }

  def dateToMonthYear(df: DataFrame): DataFrame = {
    df
      .withColumn("month", month(col("date")))
      .withColumn("year", year(col("date")))
  }

  def extractDateElements(df: DataFrame): DataFrame = {
    val dateDf = timestampToDate(df)
    val monthYearDf = dateToMonthYear(dateDf)
    monthYearDf
  }
}
