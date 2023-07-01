package providers.impl.unifiedData

import org.apache.spark.sql.DataFrame
import providers.Provider
import providers.impl.unifiedData.UnifiedTeamsHelper._
import providers.util.AverageCalculator.calculateRollingAvg
import util.constants.{CommonColumns, FixturesColumns, TemporaryRenamedColumns, UnderstatTeamsColumns, UnifiedTeamsColumns}

class UnifiedTeamsDataProvider(fixturesDf: DataFrame, understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val team1Df: DataFrame = formatTeam1Df()
    val team2Df: DataFrame = formatTeam2Df(team1Df)

    val bothTeamsScoredDf: DataFrame = bothTeamsScoredColumn(team2Df)
    val cleanSheetDf: DataFrame = cleanSheetColumns(bothTeamsScoredDf)
    val rollingAvgDf: DataFrame = applyRollingAvg(cleanSheetDf, 5)

    dropColumns(rollingAvgDf)
      .na.drop(Seq(UnifiedTeamsColumns.TEAM_1_XPX_G_AVG, UnifiedTeamsColumns.TEAM_2_XPX_G_AVG))
  }

  private def formatTeam1Df(): DataFrame = {
    val homeAwayReversedFixturesDf: DataFrame = reverseHomeAwayColumns(this.fixturesDf)
      .withColumnRenamed(FixturesColumns.HOME_TEAM, TemporaryRenamedColumns.TEAM)

    val joinedTeam1Df: DataFrame = homeAwayReversedFixturesDf
      .join(this.understatTeamsDf, Seq(CommonColumns.DATE, UnderstatTeamsColumns.TEAM), "left_outer")
      .withColumnRenamed(TemporaryRenamedColumns.TEAM, UnifiedTeamsColumns.TEAM_1_NAME)
      .na.fill(0)

    renameTeam1Columns(joinedTeam1Df)
      .withColumnRenamed(FixturesColumns.AWAY_TEAM, TemporaryRenamedColumns.TEAM)
      .drop(UnderstatTeamsColumns.SEASON)
  }

  private def formatTeam2Df(df: DataFrame) = {
    val tempUnderstat = dropPpdaColumnsDf(this.understatTeamsDf)

    val joinedTeam2Df: DataFrame = df
      .join(tempUnderstat, Seq(CommonColumns.DATE, UnderstatTeamsColumns.TEAM), "left_outer")
      .withColumnRenamed(TemporaryRenamedColumns.TEAM, UnifiedTeamsColumns.TEAM_2_NAME)
      .na.fill(0)

    renameTeam2Columns(joinedTeam2Df)
  }

  private def applyRollingAvg(df: DataFrame, numOfRows: Int): DataFrame = {
    val team1ColumnsToAvg = List(
      UnifiedTeamsColumns.TEAM_1_SCORE,
      UnifiedTeamsColumns.TEAM_1_NPX_G,
      UnifiedTeamsColumns.TEAM_1_X_G,
      UnifiedTeamsColumns.TEAM_1_NPX_G_D,
      UnifiedTeamsColumns.TEAM_1_NPX_G_A,
      UnifiedTeamsColumns.TEAM_1_X_G_A,
      UnifiedTeamsColumns.TEAM_1_CLEAN_SHEET
    )

    val team2ColumnsToAvg = List(
      UnifiedTeamsColumns.TEAM_2_SCORE,
      UnifiedTeamsColumns.TEAM_2_NPX_G,
      UnifiedTeamsColumns.TEAM_2_X_G,
      UnifiedTeamsColumns.TEAM_2_NPX_G_D,
      UnifiedTeamsColumns.TEAM_2_NPX_G_A,
      UnifiedTeamsColumns.TEAM_2_X_G_A,
      UnifiedTeamsColumns.TEAM_2_CLEAN_SHEET
    )

    val team1Avgs: DataFrame = team1ColumnsToAvg.foldLeft(df) { (currentDf, column) =>
      calculateRollingAvg(currentDf, UnifiedTeamsColumns.TEAM_1_NAME, column, numOfRows)
    }

    team2ColumnsToAvg.foldLeft(team1Avgs) { (currentDf, column) =>
      calculateRollingAvg(currentDf, UnifiedTeamsColumns.TEAM_2_NAME, column, numOfRows)
    }
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    val columnsToDrop = List(
      UnifiedTeamsColumns.TEAM_1_SCORE,
      UnifiedTeamsColumns.TEAM_2_SCORE,
      UnifiedTeamsColumns.TEAM_1_NPX_G,
      UnifiedTeamsColumns.TEAM_1_X_G,
      UnifiedTeamsColumns.TEAM_1_NPX_G_D,
      UnifiedTeamsColumns.TEAM_1_NPX_G_A,
      UnifiedTeamsColumns.TEAM_1_X_G_A,
      UnifiedTeamsColumns.TEAM_1_CLEAN_SHEET,
      UnifiedTeamsColumns.TEAM_2_NPX_G,
      UnifiedTeamsColumns.TEAM_2_X_G,
      UnifiedTeamsColumns.TEAM_2_NPX_G_D,
      UnifiedTeamsColumns.TEAM_2_NPX_G_A,
      UnifiedTeamsColumns.TEAM_2_X_G_A,
      UnifiedTeamsColumns.TEAM_2_CLEAN_SHEET
    )

    df.drop(columnsToDrop: _*)
  }
}
