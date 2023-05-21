package providers.unifiedData

import constants.{CommonColumns, FixturesColumns, UnderstatTeamsColumns}
import org.apache.spark.sql.DataFrame
import providers.Provider
import util.AverageCalculator.calculateRollingAvg
import util.DataFrameHelper.{dropNullRows, joinDataLeftOuter}

class UnifiedTeamDataProvider(fixturesDf: DataFrame, understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val homeAwayReversedFixturesDf: DataFrame = reverseHomeAwayColumns(this.fixturesDf)
    val joinedDf: DataFrame = joinDataLeftOuter(homeAwayReversedFixturesDf, this.understatTeamsDf, Seq(CommonColumns.DATE, "team"))
      .drop("hA")
    val rollingAvgDf: DataFrame = applyRollingAvg(joinedDf, 5)
    dropNullRows(rollingAvgDf, Seq("xGAvg"))
  }

  private def reverseHomeAwayColumns(fixturesDf: DataFrame): DataFrame = {
    val homeFixturesDf = extractHomeFixtures(fixturesDf)
    val awayFixturesDf = extractAwayFixtures(fixturesDf)
    val awayFixtureColsRenamedDf = renameAwayFixturesColumns(awayFixturesDf)
    homeFixturesDf.union(awayFixtureColsRenamedDf).withColumnRenamed(FixturesColumns.HOME_TEAM, "team")
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
    val awayFixtureColsRenamedDf = awayFixturesDf.withColumnRenamed(FixturesColumns.AWAY_TEAM, "homeTeam_")
      .withColumnRenamed(FixturesColumns.HOME_TEAM, "awayTeam_")
      .withColumnRenamed(FixturesColumns.TEAM_A_DIFFICULTY, "teamHDifficulty_")
      .withColumnRenamed(FixturesColumns.TEAM_A_SCORE, "teamHScore_")
      .withColumnRenamed(FixturesColumns.TEAM_H_DIFFICULTY, "teamADifficulty_")
      .withColumnRenamed(FixturesColumns.TEAM_H_SCORE, "teamAScore_")

    awayFixtureColsRenamedDf.withColumnRenamed("awayTeam_", FixturesColumns.AWAY_TEAM)
      .withColumnRenamed("homeTeam_", FixturesColumns.HOME_TEAM)
      .withColumnRenamed("teamHDifficulty_", FixturesColumns.TEAM_H_DIFFICULTY)
      .withColumnRenamed("teamHScore_", FixturesColumns.TEAM_H_SCORE)
      .withColumnRenamed("teamADifficulty_", FixturesColumns.TEAM_A_DIFFICULTY)
      .withColumnRenamed("teamAScore_", FixturesColumns.TEAM_A_SCORE)
  }

  private def applyRollingAvg(df: DataFrame, numOfRows: Int): DataFrame = {
    val xGAvgTeam1Df: DataFrame = calculateRollingAvg(df, UnderstatTeamsColumns.TEAM, UnderstatTeamsColumns.X_G, numOfRows)
    val npxGDAvgTeam1Df: DataFrame = calculateRollingAvg(xGAvgTeam1Df, UnderstatTeamsColumns.TEAM, UnderstatTeamsColumns.NPX_G_D, numOfRows)
    val npxGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGDAvgTeam1Df, UnderstatTeamsColumns.TEAM, UnderstatTeamsColumns.NPX_G_A, numOfRows)
    val xGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGAAvgTeam1Df, UnderstatTeamsColumns.TEAM, UnderstatTeamsColumns.X_G_A, numOfRows)
    val xGAvgTeam2Df: DataFrame = calculateRollingAvg(xGAAvgTeam1Df, "awayTeam", UnderstatTeamsColumns.X_G, numOfRows)
    val npxGDAvgTeam2Df: DataFrame = calculateRollingAvg(xGAvgTeam2Df, "awayTeam", UnderstatTeamsColumns.NPX_G_D, numOfRows)
    val npxGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGDAvgTeam2Df, "awayTeam", UnderstatTeamsColumns.NPX_G_A, numOfRows)
    val xGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGAAvgTeam2Df, "awayTeam", UnderstatTeamsColumns.X_G_A, numOfRows)
    xGAAvgTeam2Df
  }
}
