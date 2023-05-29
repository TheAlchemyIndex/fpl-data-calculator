package util

import constants.CommonColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, round}

object AverageCalculator {

  def calculateRollingAvg(df: DataFrame, partitionCol: String, targetCol: String, numOfRows: Int): DataFrame = {
    val window = Window.partitionBy(partitionCol).orderBy(CommonColumns.DATE).rowsBetween(-numOfRows, -1)
    df.withColumn(s"${targetCol}Avg", round(avg(targetCol).over(window), 5))
  }

  def calculateRollingAvg(df: DataFrame, partitionCol1: String, partitionCol2: String, targetCol: String, numOfRows: Int): DataFrame = {
    val window = Window.partitionBy(partitionCol1, partitionCol2).orderBy(CommonColumns.DATE).rowsBetween(-numOfRows, -1)
    df.withColumn(s"${targetCol}AvgAgainstOpponent", round(avg(targetCol).over(window), 5))
  }
}
