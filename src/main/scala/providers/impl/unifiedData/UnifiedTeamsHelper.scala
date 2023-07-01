package providers.impl.unifiedData

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import util.constants.{CommonColumns, FixturesColumns, TemporaryRenamedColumns, UnderstatTeamsColumns, UnifiedTeamsColumns}

object UnifiedTeamsHelper {

  def reverseHomeAwayColumns(df: DataFrame): DataFrame = {
    val homeFixturesDf = extractHomeFixtures(df)
    val awayFixturesDf = extractAwayFixtures(df)
    val awayFixtureColsRenamedDf = renameTeamFixturesColumns(awayFixturesDf)
    homeFixturesDf.union(awayFixtureColsRenamedDf)
  }

  def extractHomeFixtures(df: DataFrame): DataFrame = {
    df.select(CommonColumns.DATE,
      FixturesColumns.HOME_TEAM,
      FixturesColumns.AWAY_TEAM,
      FixturesColumns.TEAM_A_DIFFICULTY,
      FixturesColumns.TEAM_A_SCORE,
      FixturesColumns.TEAM_H_DIFFICULTY,
      FixturesColumns.TEAM_H_SCORE)
  }

  def extractAwayFixtures(df: DataFrame): DataFrame = {
    df.select(CommonColumns.DATE,
      FixturesColumns.AWAY_TEAM,
      FixturesColumns.HOME_TEAM,
      FixturesColumns.TEAM_H_DIFFICULTY,
      FixturesColumns.TEAM_H_SCORE,
      FixturesColumns.TEAM_A_DIFFICULTY,
      FixturesColumns.TEAM_A_SCORE)
  }

  def renameTeamFixturesColumns(df: DataFrame): DataFrame = {
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

  def renameTeam1Columns(df: DataFrame): DataFrame = {
    df.withColumnRenamed(FixturesColumns.TEAM_H_SCORE, UnifiedTeamsColumns.TEAM_1_SCORE)
      .withColumnRenamed(FixturesColumns.TEAM_H_DIFFICULTY, UnifiedTeamsColumns.TEAM_1_DIFFICULTY)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G, UnifiedTeamsColumns.TEAM_1_NPX_G)
      .withColumnRenamed(UnderstatTeamsColumns.X_G, UnifiedTeamsColumns.TEAM_1_X_G)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_D, UnifiedTeamsColumns.TEAM_1_NPX_G_D)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_A, UnifiedTeamsColumns.TEAM_1_NPX_G_A)
      .withColumnRenamed(UnderstatTeamsColumns.X_G_A, UnifiedTeamsColumns.TEAM_1_X_G_A)
  }

  def renameTeam2Columns(df: DataFrame): DataFrame = {
    df.withColumnRenamed(FixturesColumns.TEAM_A_SCORE, UnifiedTeamsColumns.TEAM_2_SCORE)
      .withColumnRenamed(FixturesColumns.TEAM_A_DIFFICULTY, UnifiedTeamsColumns.TEAM_2_DIFFICULTY)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G, UnifiedTeamsColumns.TEAM_2_NPX_G)
      .withColumnRenamed(UnderstatTeamsColumns.X_G, UnifiedTeamsColumns.TEAM_2_X_G)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_D, UnifiedTeamsColumns.TEAM_2_NPX_G_D)
      .withColumnRenamed(UnderstatTeamsColumns.NPX_G_A, UnifiedTeamsColumns.TEAM_2_NPX_G_A)
      .withColumnRenamed(UnderstatTeamsColumns.X_G_A, UnifiedTeamsColumns.TEAM_2_X_G_A)
  }

  def dropPpdaColumnsDf(df: DataFrame): DataFrame = {
    df
      .drop(UnderstatTeamsColumns.PPDA_ALLOWED_ATT)
      .drop(UnderstatTeamsColumns.PPDA_ALLOWED_DEF)
      .drop(UnderstatTeamsColumns.PPDA_ATT)
      .drop(UnderstatTeamsColumns.PPDA_DEF)
      .drop(UnderstatTeamsColumns.TEAM_1_TYPE)
      .drop(UnderstatTeamsColumns.TEAM_2_TYPE)
  }

  def bothTeamsScoredColumn(df: DataFrame): DataFrame = {
    df.withColumn(UnifiedTeamsColumns.BOTH_SCORED,
      when((col(UnifiedTeamsColumns.TEAM_1_SCORE) > 0) && (col(UnifiedTeamsColumns.TEAM_2_SCORE) > 0), 1)
        .otherwise(0))
  }

  def cleanSheetColumns(df: DataFrame): DataFrame = {
    val team1Df = df.withColumn(UnifiedTeamsColumns.TEAM_1_CLEAN_SHEET, when(col(UnifiedTeamsColumns.TEAM_2_SCORE) === 0, 1)
      when(col(UnifiedTeamsColumns.TEAM_2_SCORE) > 0, 0))

    val team2Df = team1Df.withColumn(UnifiedTeamsColumns.TEAM_2_CLEAN_SHEET, when(col(UnifiedTeamsColumns.TEAM_1_SCORE) === 0, 1)
      when(col(UnifiedTeamsColumns.TEAM_1_SCORE) > 0, 0))

    team2Df
  }
}
