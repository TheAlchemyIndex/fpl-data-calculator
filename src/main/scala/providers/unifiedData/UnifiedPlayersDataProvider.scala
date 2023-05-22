package providers.unifiedData

import constants.{CalculatedColumns, CommonColumns, GameweekColumns, UnderstatPlayersColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg
import providers.Provider
import util.AverageCalculator.calculateRollingAvg
import util.DataFrameHelper.{dropNullRows, joinDataLeftOuter}

class UnifiedPlayersDataProvider(gameweekDf: DataFrame, understatDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val joinedDf: DataFrame = joinDataLeftOuter(this.gameweekDf, this.understatDf, Seq(CommonColumns.NAME, CommonColumns.DATE))
    val rollingAvgDf: DataFrame = applyRollingAvg(joinedDf, 5)
    val filteredColumnsDf: DataFrame = dropColumns(rollingAvgDf)
    val avgAgainstOpponentsDf: DataFrame = calculateAvgAgainstOpponent(filteredColumnsDf, GameweekColumns.TOTAL_POINTS).orderBy(CommonColumns.DATE)
    dropNullRows(avgAgainstOpponentsDf, Seq(CalculatedColumns.BONUS_AVG))
  }

  private def applyRollingAvg(df: DataFrame, numOfRows: Int) = {
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
    val npxGAvgDf: DataFrame = calculateRollingAvg(ictIndexAvgDf, CommonColumns.NAME, UnderstatPlayersColumns.NPX_G, numOfRows)
    val keyPassesAvgDf: DataFrame = calculateRollingAvg(npxGAvgDf, CommonColumns.NAME, UnderstatPlayersColumns.KEY_PASSES, numOfRows)
    val npgAvgDf: DataFrame = calculateRollingAvg(keyPassesAvgDf, CommonColumns.NAME, UnderstatPlayersColumns.NPG, numOfRows)
    val xAAvgDf: DataFrame = calculateRollingAvg(npgAvgDf, CommonColumns.NAME, UnderstatPlayersColumns.X_A, numOfRows)
    val xGAvgDf: DataFrame = calculateRollingAvg(xAAvgDf, CommonColumns.NAME, UnderstatPlayersColumns.X_G, numOfRows)
    val shotsAvgDf: DataFrame = calculateRollingAvg(xGAvgDf, CommonColumns.NAME, UnderstatPlayersColumns.SHOTS, numOfRows)
    val xGBuildupAvgDf: DataFrame = calculateRollingAvg(shotsAvgDf, CommonColumns.NAME, UnderstatPlayersColumns.X_G_BUILDUP, numOfRows)
    xGBuildupAvgDf
  }

  private def dropColumns(df: DataFrame): DataFrame = {
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
      UnderstatPlayersColumns.NPX_G,
      UnderstatPlayersColumns.KEY_PASSES,
      UnderstatPlayersColumns.NPG,
      UnderstatPlayersColumns.X_A,
      UnderstatPlayersColumns.X_G,
      UnderstatPlayersColumns.SHOTS,
      UnderstatPlayersColumns.X_G_BUILDUP)
  }

  private def calculateAvgAgainstOpponent(df: DataFrame, targetCol: String): DataFrame = {
    val window = Window.partitionBy(CommonColumns.NAME, GameweekColumns.OPPONENT_TEAM)
    df.withColumn(s"${targetCol}AgainstOpponentAvg", avg(targetCol).over(window))
  }
}
