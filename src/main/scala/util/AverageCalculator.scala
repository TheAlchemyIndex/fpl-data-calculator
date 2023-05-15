package util

import constants.{CommonColumns, GameweekColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, round}

object AverageCalculator {

  def calculateRollingAvg(df: DataFrame, partitionCol: String, targetCol: String, numOfRows: Long): DataFrame = {
    val window = Window.partitionBy(partitionCol).orderBy(CommonColumns.DATE).rowsBetween(-numOfRows, -1)
    df.withColumn(s"${targetCol}Avg", round(avg(targetCol).over(window), 5))
  }

  def calculateAvgAgainstOpponent(df: DataFrame, targetCol: String): DataFrame = {
    val window = Window.partitionBy(CommonColumns.NAME, GameweekColumns.OPPONENT_TEAM)
    df.withColumn(s"${targetCol}AgainstOpponentAvg", avg(targetCol).over(window))
  }
}
