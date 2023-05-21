package providers.understat.teams

import constants.{CommonColumns, UnderstatTeamsColumns}
import org.apache.spark.sql.DataFrame
import providers.Provider
import util.DataFrameHelper.renameColumnsToCamelCase
import util.DateFormatter.timestampToDate

class UnderstatTeamsProvider(understatTeamsDf: DataFrame) extends Provider {

  def getData: DataFrame = {
    val camelCaseDf: DataFrame = renameColumnsToCamelCase(understatTeamsDf)
    val timestampToDateDf: DataFrame = timestampToDate(camelCaseDf, CommonColumns.DATE, UnderstatTeamsColumns.DATE)
    dropColumns(timestampToDateDf)
  }

  private def dropColumns(df: DataFrame): DataFrame = {
    df.drop(UnderstatTeamsColumns.WINS,
      UnderstatTeamsColumns.DEEP,
      UnderstatTeamsColumns.PPDA_ALLOWED,
      UnderstatTeamsColumns.MISSED,
      UnderstatTeamsColumns.PPDA,
      UnderstatTeamsColumns.PTS,
      UnderstatTeamsColumns.XPTS,
      UnderstatTeamsColumns.RESULT,
      UnderstatTeamsColumns.DEEP_ALLOWED,
      UnderstatTeamsColumns.SCORED,
      UnderstatTeamsColumns.LOSES,
      UnderstatTeamsColumns.DRAWS
    )
  }
}
