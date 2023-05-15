package unifiedData

import constants.{CalculatedColumns, CommonColumns, GameweekColumns, UnderstatColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import util.AverageCalculator.calculateRollingAvg

object UnifiedDataHelper {

  def applyRollingAvg(df: DataFrame, numOfRows: Long): DataFrame = {
    val bonusAvgDf: DataFrame = calculateRollingAvg(df, CommonColumns.NAME, GameweekColumns.BONUS, numOfRows)
    val cleanSheetsAvgDf: DataFrame = calculateRollingAvg(bonusAvgDf, CommonColumns.NAME, GameweekColumns.CLEAN_SHEETS, numOfRows)
    val goalsConcededAvgDf: DataFrame = calculateRollingAvg(cleanSheetsAvgDf, CommonColumns.NAME, GameweekColumns.GOALS_CONCEDED, numOfRows)
    val totalPointsAvgDf: DataFrame = calculateRollingAvg(goalsConcededAvgDf, CommonColumns.NAME, GameweekColumns.TOTAL_POINTS, numOfRows)
    val influenceAvgDf: DataFrame = calculateRollingAvg(totalPointsAvgDf, CommonColumns.NAME, GameweekColumns.INFLUENCE, numOfRows)
    val assistsAvgDf: DataFrame = calculateRollingAvg(influenceAvgDf, CommonColumns.NAME, GameweekColumns.ASSISTS, numOfRows)
    val creativityAvgDf: DataFrame = calculateRollingAvg(assistsAvgDf, CommonColumns.NAME, GameweekColumns.CREATIVITY, numOfRows)
    val valueAvgDf: DataFrame = calculateRollingAvg(creativityAvgDf, CommonColumns.NAME, GameweekColumns.VALUE, numOfRows)
    val goalsScoredAvgDf: DataFrame = calculateRollingAvg(valueAvgDf, CommonColumns.NAME, GameweekColumns.GOALS_SCORED, numOfRows)
    val minutesAvgDf: DataFrame = calculateRollingAvg(goalsScoredAvgDf, CommonColumns.NAME, GameweekColumns.MINUTES, numOfRows)
    val yellowCardsScoredAvgDf: DataFrame = calculateRollingAvg(minutesAvgDf, CommonColumns.NAME, GameweekColumns.YELLOW_CARDS, numOfRows)
    val threatAvgDf: DataFrame = calculateRollingAvg(yellowCardsScoredAvgDf, CommonColumns.NAME, GameweekColumns.THREAT, numOfRows)
    val ictIndexAvgDf: DataFrame = calculateRollingAvg(threatAvgDf, CommonColumns.NAME, GameweekColumns.ICT_INDEX, numOfRows)
    val npxGAvgDf: DataFrame = calculateRollingAvg(ictIndexAvgDf, CommonColumns.NAME, UnderstatColumns.NPX_G, numOfRows)
    val keyPassesAvgDf: DataFrame = calculateRollingAvg(npxGAvgDf, CommonColumns.NAME, UnderstatColumns.KEY_PASSES, numOfRows)
    val npgAvgDf: DataFrame = calculateRollingAvg(keyPassesAvgDf, CommonColumns.NAME, UnderstatColumns.NPG, numOfRows)
    val xAAvgDf: DataFrame = calculateRollingAvg(npgAvgDf, CommonColumns.NAME, UnderstatColumns.X_A, numOfRows)
    val xGAvgDf: DataFrame = calculateRollingAvg(xAAvgDf, CommonColumns.NAME, UnderstatColumns.X_G, numOfRows)
    val shotsAvgDf: DataFrame = calculateRollingAvg(xGAvgDf, CommonColumns.NAME, UnderstatColumns.SHOTS, numOfRows)
    val xGBuildupAvgDf: DataFrame = calculateRollingAvg(shotsAvgDf, CommonColumns.NAME, UnderstatColumns.X_G_BUILDUP, numOfRows)
    xGBuildupAvgDf
  }

  def dropColumnsAfterAvg(df: DataFrame): DataFrame = {
    df.drop(GameweekColumns.BONUS,
        GameweekColumns.CLEAN_SHEETS,
        GameweekColumns.GOALS_CONCEDED,
        GameweekColumns.TEAM_A_SCORE,
        GameweekColumns.INFLUENCE,
        GameweekColumns.TRANSFERS_IN,
        GameweekColumns.SAVES,
        GameweekColumns.ASSISTS,
        GameweekColumns.CREATIVITY,
        GameweekColumns.VALUE,
        GameweekColumns.SELECTED,
        GameweekColumns.GOALS_SCORED,
        GameweekColumns.YELLOW_CARDS,
        GameweekColumns.TRANSFERS_OUT,
        GameweekColumns.THREAT,
        GameweekColumns.ICT_INDEX,
        GameweekColumns.PENALTIES_SAVED,
        GameweekColumns.TEAM_H_SCORE,
        UnderstatColumns.NPX_G,
        UnderstatColumns.KEY_PASSES,
        UnderstatColumns.NPG,
        UnderstatColumns.X_A,
        UnderstatColumns.X_G,
        UnderstatColumns.SHOTS,
        UnderstatColumns.X_G_BUILDUP)
  }

  def dropNullAvgs(df: DataFrame): DataFrame = {
    df.na.drop(Seq(CalculatedColumns.BONUS_AVG))
      .orderBy(col(CommonColumns.DATE).asc)
  }
}
