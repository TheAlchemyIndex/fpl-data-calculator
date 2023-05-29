package providers.unifiedData

import constants.{CommonColumns, FixturesColumns, TemporaryRenamedColumns, UnderstatTeamsColumns, UnifiedTeamsColumns}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import providers.Provider
import util.AverageCalculator.calculateRollingAvg
import util.DataFrameHelper.{dropNullRows, joinDataLeftOuter}

class UnifiedTeamsDataProvider(fixturesDf: DataFrame, understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val homeAwayReversedFixturesDf: DataFrame = reverseHomeAwayColumns(this.fixturesDf)
      .withColumnRenamed(FixturesColumns.HOME_TEAM, TemporaryRenamedColumns.TEAM)
    val joinedTeam1Df: DataFrame = joinDataLeftOuter(homeAwayReversedFixturesDf, this.understatTeamsDf, Seq(CommonColumns.DATE, UnderstatTeamsColumns.TEAM))
      .withColumnRenamed(TemporaryRenamedColumns.TEAM, UnifiedTeamsColumns.TEAM_1)
      .drop(UnderstatTeamsColumns.H_A)

    val team1RenamedXGColumnsDf: DataFrame = renameTeam1Columns(joinedTeam1Df)
      .withColumnRenamed(FixturesColumns.AWAY_TEAM, TemporaryRenamedColumns.TEAM)
      .drop(UnderstatTeamsColumns.SEASON)

    val joinedTeam2Df: DataFrame = joinDataLeftOuter(team1RenamedXGColumnsDf, this.understatTeamsDf, Seq(CommonColumns.DATE, UnderstatTeamsColumns.TEAM))
      .withColumnRenamed(TemporaryRenamedColumns.TEAM, UnifiedTeamsColumns.TEAM_2)
      .drop(UnderstatTeamsColumns.H_A)

    val team2RenamedXGColumnsDf: DataFrame = renameTeam2Columns(joinedTeam2Df)
    val bothTeamsScoredDf: DataFrame = bothTeamsScoredColumn(team2RenamedXGColumnsDf)

    val rollingAvgDf: DataFrame = applyRollingAvg(bothTeamsScoredDf, 5)
    val rollingAvgAgainstOpponentDf: DataFrame = applyRollingAvgAgainstOpponent(rollingAvgDf, 5)
    val droppedColumnsDf: DataFrame = dropColumns(rollingAvgAgainstOpponentDf)
    dropNullRows(droppedColumnsDf, Seq(UnifiedTeamsColumns.TEAM_1_XPX_G_AVG, UnifiedTeamsColumns.TEAM_2_XPX_G_AVG, UnifiedTeamsColumns.TEAM_1_SCORE_AVG_OPPONENT))
  }

  private def reverseHomeAwayColumns(df: DataFrame): DataFrame = {
    val homeFixturesDf = extractHomeFixtures(df)
    val awayFixturesDf = extractAwayFixtures(df)
    val awayFixtureColsRenamedDf = renameTeamFixturesColumns(awayFixturesDf)
    homeFixturesDf.union(awayFixtureColsRenamedDf)
  }

  private def extractHomeFixtures(df: DataFrame): DataFrame = {
    df.select(CommonColumns.DATE,
      FixturesColumns.HOME_TEAM,
      FixturesColumns.AWAY_TEAM,
      FixturesColumns.TEAM_A_DIFFICULTY,
      FixturesColumns.TEAM_A_SCORE,
      FixturesColumns.TEAM_H_DIFFICULTY,
      FixturesColumns.TEAM_H_SCORE)
  }

  private def extractAwayFixtures(df: DataFrame): DataFrame = {
    df.select(CommonColumns.DATE,
      FixturesColumns.AWAY_TEAM,
      FixturesColumns.HOME_TEAM,
      FixturesColumns.TEAM_H_DIFFICULTY,
      FixturesColumns.TEAM_H_SCORE,
      FixturesColumns.TEAM_A_DIFFICULTY,
      FixturesColumns.TEAM_A_SCORE)
  }

  private def renameTeamFixturesColumns(df: DataFrame): DataFrame = {
    val awayFixtureColsRenamedDf = df.withColumnRenamed(FixturesColumns.AWAY_TEAM, TemporaryRenamedColumns.HOME_TEAM_TEMP)
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

  private def renameTeam1Columns(df: DataFrame): DataFrame = {
    df.withColumnRenamed(FixturesColumns.TEAM_H_SCORE, UnifiedTeamsColumns.TEAM_1_SCORE)
      .withColumnRenamed(FixturesColumns.TEAM_H_DIFFICULTY, UnifiedTeamsColumns.TEAM_1_DIFFICULTY)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G, UnifiedTeamsColumns.TEAM_1_NPX_G)
      .withColumnRenamed(UnderstatTeamsColumns.X_G, UnifiedTeamsColumns.TEAM_1_X_G)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_D, UnifiedTeamsColumns.TEAM_1_NPX_G_D)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_A, UnifiedTeamsColumns.TEAM_1_NPX_G_A)
      .withColumnRenamed(UnderstatTeamsColumns.X_G_A, UnifiedTeamsColumns.TEAM_1_X_G_A)
  }

  private def renameTeam2Columns(df: DataFrame): DataFrame = {
    df.withColumnRenamed(FixturesColumns.TEAM_A_SCORE, UnifiedTeamsColumns.TEAM_2_SCORE)
      .withColumnRenamed(FixturesColumns.TEAM_A_DIFFICULTY, UnifiedTeamsColumns.TEAM_2_DIFFICULTY)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G, UnifiedTeamsColumns.TEAM_2_NPX_G)
      .withColumnRenamed(UnderstatTeamsColumns.X_G, UnifiedTeamsColumns.TEAM_2_X_G)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_D, UnifiedTeamsColumns.TEAM_2_NPX_G_D)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_A, UnifiedTeamsColumns.TEAM_2_NPX_G_A)
      .withColumnRenamed(UnderstatTeamsColumns.X_G_A, UnifiedTeamsColumns.TEAM_2_X_G_A)
  }

  private def bothTeamsScoredColumn(df: DataFrame): DataFrame = {
    df.withColumn(UnifiedTeamsColumns.BOTH_SCORED,
      when((col(UnifiedTeamsColumns.TEAM_1_SCORE) > 0) && (col(UnifiedTeamsColumns.TEAM_2_SCORE) > 0), 1)
        .otherwise(0))
  }

  private def applyRollingAvg(df: DataFrame, numOfRows: Int): DataFrame = {
    val goalsScoredAvgTeam1Df: DataFrame = calculateRollingAvg(df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_1_SCORE, numOfRows)
    val xpxGAvgTeam1Df: DataFrame = calculateRollingAvg(goalsScoredAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_1_NPX_G, numOfRows)
    val xGAvgTeam1Df: DataFrame = calculateRollingAvg(xpxGAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_1_X_G, numOfRows)
    val npxGDvgTeam1Df: DataFrame = calculateRollingAvg(xGAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_1_NPX_G_D, numOfRows)
    val npxGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGDvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_1_NPX_G_A, numOfRows)
    val xGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGAAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_1_X_G_A, numOfRows)

    val goalsScoredAvgTeam2Df: DataFrame = calculateRollingAvg(xGAAvgTeam1Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_2_SCORE, numOfRows)
    val xpxGAvgTeam2Df: DataFrame = calculateRollingAvg(goalsScoredAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_2_NPX_G, numOfRows)
    val xGAvgTeam2Df: DataFrame = calculateRollingAvg(xpxGAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_2_X_G, numOfRows)
    val npxGDvgTeam2Df: DataFrame = calculateRollingAvg(xGAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_2_NPX_G_D, numOfRows)
    val npxGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGDvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_2_NPX_G_A, numOfRows)
    val xGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGAAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_2_X_G_A, numOfRows)
    xGAAvgTeam2Df
  }

  private def applyRollingAvgAgainstOpponent(df: DataFrame, numOfRows: Int): DataFrame = {
    val goalsScoredAvgTeam1Df: DataFrame = calculateRollingAvg(df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1_SCORE, numOfRows)
    val xpxGAvgTeam1Df: DataFrame = calculateRollingAvg(goalsScoredAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1_NPX_G, numOfRows)
    val xGAvgTeam1Df: DataFrame = calculateRollingAvg(xpxGAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1_X_G, numOfRows)
    val npxGDvgTeam1Df: DataFrame = calculateRollingAvg(xGAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1_NPX_G_D, numOfRows)
    val npxGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGDvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1_NPX_G_A, numOfRows)
    val xGAAvgTeam1Df: DataFrame = calculateRollingAvg(npxGAAvgTeam1Df, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1_X_G_A, numOfRows)

    val goalsScoredAvgTeam2Df: DataFrame = calculateRollingAvg(xGAAvgTeam1Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2_SCORE, numOfRows)
    val xpxGAvgTeam2Df: DataFrame = calculateRollingAvg(goalsScoredAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2_NPX_G, numOfRows)
    val xGAvgTeam2Df: DataFrame = calculateRollingAvg(xpxGAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2_X_G, numOfRows)
    val npxGDvgTeam2Df: DataFrame = calculateRollingAvg(xGAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2_NPX_G_D, numOfRows)
    val npxGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGDvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2_NPX_G_A, numOfRows)
    val xGAAvgTeam2Df: DataFrame = calculateRollingAvg(npxGAAvgTeam2Df, UnifiedTeamsColumns.TEAM_2, UnifiedTeamsColumns.TEAM_1, UnifiedTeamsColumns.TEAM_2_X_G_A, numOfRows)
    xGAAvgTeam2Df
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    df.drop(UnifiedTeamsColumns.TEAM_1_SCORE,
      UnifiedTeamsColumns.TEAM_2_SCORE,
      UnifiedTeamsColumns.TEAM_1_NPX_G,
      UnifiedTeamsColumns.TEAM_1_X_G,
      UnifiedTeamsColumns.TEAM_1_NPX_G_D,
      UnifiedTeamsColumns.TEAM_1_NPX_G_A,
      UnifiedTeamsColumns.TEAM_1_X_G_A,
      UnifiedTeamsColumns.TEAM_2_NPX_G,
      UnifiedTeamsColumns.TEAM_2_X_G,
      UnifiedTeamsColumns.TEAM_2_NPX_G_D,
      UnifiedTeamsColumns.TEAM_2_NPX_G_A,
      UnifiedTeamsColumns.TEAM_2_X_G_A
    )
  }
}
