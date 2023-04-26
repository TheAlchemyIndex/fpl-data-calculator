import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg

object RollingAverage {

  def calculateRollingAvg(df: DataFrame, partitionCol: String, targetCol: String, numOfRows: Long): DataFrame = {
    val windowSpec = Window.partitionBy(partitionCol).orderBy("date").rowsBetween(-(numOfRows - 1), 0)
    val rollingAvgDF = df.withColumn(s"${targetCol}_avg", avg(targetCol).over(windowSpec))
    rollingAvgDF
  }

  def applyRollingAvg(df: DataFrame, numOfRows: Long): DataFrame = {
    val bonusAvgDf: DataFrame = calculateRollingAvg(df, "name", "bonus", numOfRows)
    val cleanSheetsAvgDf: DataFrame = calculateRollingAvg(bonusAvgDf, "name", "clean_sheets", numOfRows)
    val goalsConcededAvgDf: DataFrame = calculateRollingAvg(cleanSheetsAvgDf, "name", "goals_conceded", numOfRows)
    val totalPointsAvgDf: DataFrame = calculateRollingAvg(goalsConcededAvgDf, "name", "total_points", numOfRows)
    val influenceAvgDf: DataFrame = calculateRollingAvg(totalPointsAvgDf, "name", "influence", numOfRows)
    val assistsAvgDf: DataFrame = calculateRollingAvg(influenceAvgDf, "name", "assists", numOfRows)
//    val xpAvgDf: DataFrame = calculateRollingAvg(assistsAvgDf, "name", "xP", numOfRows)
    val creativityAvgDf: DataFrame = calculateRollingAvg(assistsAvgDf, "name", "creativity", numOfRows)
    val valueAvgDf: DataFrame = calculateRollingAvg(creativityAvgDf, "name", "value", numOfRows)
    val goalsScoredAvgDf: DataFrame = calculateRollingAvg(valueAvgDf, "name", "goals_scored", numOfRows)
    val minutesAvgDf: DataFrame = calculateRollingAvg(goalsScoredAvgDf, "name", "minutes", numOfRows)
    val yellowCardsScoredAvgDf: DataFrame = calculateRollingAvg(minutesAvgDf, "name", "yellow_cards", numOfRows)
    val threatAvgDf: DataFrame = calculateRollingAvg(yellowCardsScoredAvgDf, "name", "threat", numOfRows)
    val ictIndexAvgDf: DataFrame = calculateRollingAvg(threatAvgDf, "name", "ict_index", numOfRows)
    ictIndexAvgDf
  }
}
