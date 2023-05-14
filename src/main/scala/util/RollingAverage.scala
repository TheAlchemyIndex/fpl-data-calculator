package util

import constants.CommonColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, round}

object RollingAverage {

  def calculateRollingAvg(df: DataFrame, partitionCol: String, targetCol: String, numOfRows: Long): DataFrame = {
    val window = Window.partitionBy(partitionCol).orderBy(CommonColumns.DATE).rowsBetween(-numOfRows, -1)
    val rollingAvgDF = df.withColumn(s"${targetCol}Avg", round(avg(targetCol).over(window), 5))
    rollingAvgDF
  }
}
