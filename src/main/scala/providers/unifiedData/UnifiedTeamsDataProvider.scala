package providers.unifiedData

import constants.{CommonColumns, FixturesColumns, TemporaryRenamedColumns, UnderstatTeamsColumns, UnifiedTeamsColumns}
import org.apache.spark.sql.DataFrame
import providers.Provider
import util.AverageCalculator.calculateRollingAvg
import util.DataFrameHelper.{dropNullRows, joinDataLeftOuter}

class UnifiedTeamsDataProvider(fixturesDf: DataFrame, understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val homeAwayReversedFixturesDf: DataFrame = reverseHomeAwayColumns(this.fixturesDf)
    val joinedDf: DataFrame = joinDataLeftOuter(homeAwayReversedFixturesDf, this.understatTeamsDf, Seq(CommonColumns.DATE, UnifiedTeamsColumns.TEAM))
      .drop(UnderstatTeamsColumns.H_A)
    val rollingAvgDf: DataFrame = applyRollingAvg(joinedDf, 5)
    dropNullRows(rollingAvgDf, Seq(UnifiedTeamsColumns.X_G_AVG))
  }

  private def reverseHomeAwayColumns(fixturesDf: DataFrame): DataFrame = {
    val homeFixturesDf = extractHomeFixtures(fixturesDf)
    val awayFixturesDf = extractAwayFixtures(fixturesDf)
    val awayFixtureColsRenamedDf = renameAwayFixturesColumns(awayFixturesDf)
    homeFixturesDf.union(awayFixtureColsRenamedDf).withColumnRenamed(FixturesColumns.HOME_TEAM, UnifiedTeamsColumns.TEAM)
  }

  private def extractHomeFixtures(fixturesDf: DataFrame): DataFrame = {
    fixturesDf.select(CommonColumns.DATE,
      FixturesColumns.HOME_TEAM,
      FixturesColumns.AWAY_TEAM,
      FixturesColumns.TEAM_A_DIFFICULTY,
      FixturesColumns.TEAM_A_SCORE,
      FixturesColumns.TEAM_H_DIFFICULTY,
      FixturesColumns.TEAM_H_SCORE)
  }

  private def extractAwayFixtures(fixturesDf: DataFrame): DataFrame = {
    fixturesDf.select(CommonColumns.DATE,
      FixturesColumns.AWAY_TEAM,
      FixturesColumns.HOME_TEAM,
      FixturesColumns.TEAM_H_DIFFICULTY,
      FixturesColumns.TEAM_H_SCORE,
      FixturesColumns.TEAM_A_DIFFICULTY,
      FixturesColumns.TEAM_A_SCORE)
  }

  private def renameAwayFixturesColumns(awayFixturesDf: DataFrame): DataFrame = {
    val awayFixtureColsRenamedDf = awayFixturesDf.withColumnRenamed(FixturesColumns.AWAY_TEAM, TemporaryRenamedColumns.HOME_TEAM_TEMP)
      .withColumnRenamed(FixturesColumns.HOME_TEAM, TemporaryRenamedColumns.AWAY_TEAM_TEMP)
      .withColumnRenamed(FixturesColumns.TEAM_A_DIFFICULTY, TemporaryRenamedColumns.TEAM_H_DIFFICULTY_TEMP)
      .withColumnRenamed(FixturesColumns.TEAM_A_SCORE, TemporaryRenamedColumns.TEAM_H_SCORE_TEMP)
      .withColumnRenamed(FixturesColumns.TEAM_H_DIFFICULTY, TemporaryRenamedColumns.TEAM_A_DIFFICULTY_TEMP)
      .withColumnRenamed(FixturesColumns.TEAM_H_SCORE, TemporaryRenamedColumns.TEAM_A_SCORE_TEMP)

    awayFixtureColsRenamedDf.withColumnRenamed(TemporaryRenamedColumns.AWAY_TEAM_TEMP, FixturesColumns.AWAY_TEAM)
      .withColumnRenamed(TemporaryRenamedColumns.HOME_TEAM_TEMP, FixturesColumns.HOME_TEAM)
      .withColumnRenamed(TemporaryRenamedColumns.TEAM_H_DIFFICULTY_TEMP, FixturesColumns.TEAM_H_DIFFICULTY)
      .withColumnRenamed(TemporaryRenamedColumns.TEAM_H_SCORE_TEMP, FixturesColumns.TEAM_H_SCORE)
      .withColumnRenamed(TemporaryRenamedColumns.TEAM_A_DIFFICULTY_TEMP, FixturesColumns.TEAM_A_DIFFICULTY)
      .withColumnRenamed(TemporaryRenamedColumns.TEAM_A_SCORE_TEMP, FixturesColumns.TEAM_A_SCORE)
  }

  private def applyRollingAvg(df: DataFrame, numOfRows: Int): DataFrame = {
    val xGAvgTeam1Df: DataFrame = calculateRollingAvg(df, UnifiedTeamsColumns.TEAM, UnderstatTeamsColumns.X_G, numOfRows)
    val npxGDAvgTeam1Df: DataFrame = calculateRollingAvg(xGAvgTeam1Df, UnifiedTeamsColumns.TEAM, UnderstatTeamsColumns.NPX_G_D, numOfRows)
    val npxGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGDAvgTeam1Df, UnifiedTeamsColumns.TEAM, UnderstatTeamsColumns.NPX_G_A, numOfRows)
    val xGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGAAvgTeam1Df, UnifiedTeamsColumns.TEAM, UnderstatTeamsColumns.X_G_A, numOfRows)
    val xGAvgTeam2Df: DataFrame = calculateRollingAvg(xGAAvgTeam1Df, UnifiedTeamsColumns.AWAY_TEAM, UnderstatTeamsColumns.X_G, numOfRows)
    val npxGDAvgTeam2Df: DataFrame = calculateRollingAvg(xGAvgTeam2Df, UnifiedTeamsColumns.AWAY_TEAM, UnderstatTeamsColumns.NPX_G_D, numOfRows)
    val npxGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGDAvgTeam2Df, UnifiedTeamsColumns.AWAY_TEAM, UnderstatTeamsColumns.NPX_G_A, numOfRows)
    val xGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGAAvgTeam2Df, UnifiedTeamsColumns.AWAY_TEAM, UnderstatTeamsColumns.X_G_A, numOfRows)
    xGAAvgTeam2Df
  }
}
